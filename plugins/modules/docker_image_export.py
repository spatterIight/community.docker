#!/usr/bin/python
#
# Copyright (c) 2023, Felix Fontein <felix@fontein.de>
# GNU General Public License v3.0+ (see LICENSES/GPL-3.0-or-later.txt or https://www.gnu.org/licenses/gpl-3.0.txt)
# SPDX-License-Identifier: GPL-3.0-or-later

from __future__ import annotations

DOCUMENTATION = r"""
module: docker_image_export

short_description: Export (archive) Docker images

version_added: 3.7.0

description:
  - Creates an archive (tarball) from one or more Docker images.
  - This can be copied to another machine and loaded with M(community.docker.docker_image_load).
extends_documentation_fragment:
  - community.docker._docker.api_documentation
  - community.docker._attributes
  - community.docker._attributes.actiongroup_docker

attributes:
  check_mode:
    support: full
  diff_mode:
    support: none
  idempotent:
    support: full

options:
  names:
    description:
      - 'One or more image names. Name format will be one of: C(name), C(repository/name), C(registry_server:port/name). When
        pushing or pulling an image the name can optionally include the tag by appending C(:tag_name).'
      - Note that image IDs (hashes) can also be used.
    type: list
    elements: str
    required: true
    aliases:
      - name
  tag:
    description:
      - Tag for the image name O(name) that is to be tagged.
      - If O(name)'s format is C(name:tag), then the tag value from O(name) will take precedence.
    type: str
    default: latest
  path:
    description:
      - The C(.tar) file the image should be exported to.
    type: path
  force:
    description:
      - Export the image even if the C(.tar) file already exists and seems to contain the right image.
    type: bool
    default: false
  platform:
    description:
      - Ask for this specific platform when exporting.
      - For example, C(linux/amd64), C(linux/arm64).
      - Requires Docker API 1.48 or newer.
    type: str
    version_added: 5.2.0

requirements:
  - "Docker API >= 1.25"

author:
  - Felix Fontein (@felixfontein)

seealso:
  - module: community.docker.docker_image
  - module: community.docker.docker_image_info
  - module: community.docker.docker_image_load
"""

EXAMPLES = r"""
---
- name: Export an image
  community.docker.docker_image_export:
    name: pacur/centos-7
    path: /tmp/centos-7.tar

- name: Export multiple images
  community.docker.docker_image_export:
    names:
      - hello-world:latest
      - pacur/centos-7:latest
    path: /tmp/various.tar
"""

RETURN = r"""
images:
  description: Image inspection results for the affected images.
  returned: success
  type: list
  elements: dict
  sample: []
"""

import json
import traceback
import typing as t

from ansible_collections.community.docker.plugins.module_utils._api.constants import (
    DEFAULT_DATA_CHUNK_SIZE,
)
from ansible_collections.community.docker.plugins.module_utils._api.errors import (
    DockerException,
)
from ansible_collections.community.docker.plugins.module_utils._api.utils.utils import (
    parse_repository_tag,
)
from ansible_collections.community.docker.plugins.module_utils._common_api import (
    AnsibleDockerClient,
    RequestException,
)
from ansible_collections.community.docker.plugins.module_utils._image_archive import (
    ImageArchiveInvalidException,
    api_image_id,
    load_archived_image_manifest,
)
from ansible_collections.community.docker.plugins.module_utils._platform import (
    _Platform,
)
from ansible_collections.community.docker.plugins.module_utils._util import (
    DockerBaseClass,
    is_image_name_id,
    is_valid_tag,
)


def _canonical_name_from_repo_tags(repo_tags: list[str], requested_tag: str) -> str | None:
    """
    Return the single RepoTag whose suffix matches ``:<requested_tag>``, if exactly one exists.

    Docker normalises image names at pull time (e.g. ``docker.io/library/busybox`` →
    ``busybox``), so ``image["RepoTags"]`` already contains the canonical form that will
    appear in the archive's ``manifest.json``.  By finding the matching tag here we avoid
    hard-coding any registry-specific prefix rules.

    Returns ``None`` when zero or more than one tag matches; the caller should fall back
    to the user-provided name in that case.
    """
    suffix = f":{requested_tag}"
    matches = [t for t in repo_tags if t.endswith(suffix)]
    return matches[0] if len(matches) == 1 else None


class ImageExportManager(DockerBaseClass):
    def __init__(self, client: AnsibleDockerClient) -> None:
        super().__init__()

        self.client = client
        parameters = self.client.module.params
        self.check_mode = self.client.check_mode

        self.path = parameters["path"]
        self.force = parameters["force"]
        self.tag = parameters["tag"]
        self.platform = parameters["platform"]

        if not is_valid_tag(self.tag, allow_empty=True):
            self.fail(f'"{self.tag}" is not a valid docker tag')

        # If name contains a tag, it takes precedence over tag parameter.
        self.names = []
        for name in parameters["names"]:
            if is_image_name_id(name):
                self.names.append({"id": name, "joined": name})
            else:
                repo, repo_tag = parse_repository_tag(name)
                if not repo_tag:
                    repo_tag = self.tag
                self.names.append(
                    {"name": repo, "tag": repo_tag, "joined": f"{repo}:{repo_tag}"}
                )

        if not self.names:
            self.fail("At least one image name must be specified")

    def fail(self, msg: str) -> t.NoReturn:
        self.client.fail(msg)

    def get_export_reason(self) -> str | None:
        if self.force:
            return "Exporting since force=true"

        try:
            archived_images = load_archived_image_manifest(self.path)
            if archived_images is None:
                return "Overwriting since no image is present in archive"
        except ImageArchiveInvalidException as exc:
            self.log(f"Unable to extract manifest summary from archive: {exc}")
            return "Overwriting an unreadable archive file"

        # Parse the requested platform once (if specified) for use in the loop below.
        requested_platform: _Platform | None = None
        if self.platform:
            try:
                requested_platform = _Platform.parse_platform_string(self.platform)
            except ValueError:
                pass  # already validated in __init__; treat as no platform filter

        # Use bipartite matching: each archive entry must be covered by at least one
        # requested name, and each requested name must be covered by at least one
        # archive entry. This correctly handles the same image appearing multiple
        # times in a request (e.g., by both ID and name).
        matches: list[tuple[int, int]] = []  # (archive_idx, name_idx)
        for ai, archived_image in enumerate(archived_images):
            archived_repo_tags = archived_image.repo_tags or []
            for ni, name in enumerate(self.names):
                if "name" not in name:
                    # ID-based request: compare hashes directly (reliable on all Docker versions).
                    id_matches = name["id"] == api_image_id(archived_image.image_id) or (
                        archived_image.manifest_id is not None
                        and name["id"] == api_image_id(archived_image.manifest_id)
                    )
                elif requested_platform is not None and archived_image.platform is not None:
                    # Name-based request with platform filter: compare platforms instead of
                    # image IDs. On Docker 29, inspect returns the image-index digest which
                    # never matches the per-platform manifest hash stored in the archive.
                    # Known limitation: image content updates are not detected; use force=true.
                    try:
                        id_matches = (
                            _Platform.parse_platform_string(archived_image.platform)
                            == requested_platform
                        )
                    except ValueError:
                        id_matches = False
                else:
                    # Name-based request without platform, or platform not extractable from
                    # archive (old format): fall back to existing image-ID hash comparison.
                    id_matches = name["id"] == api_image_id(archived_image.image_id) or (
                        archived_image.manifest_id is not None
                        and name["id"] == api_image_id(archived_image.manifest_id)
                    )
                # For requests by image ID (no repo name), repo tags are irrelevant —
                # only the image ID needs to match. For requests by name, compare the
                # canonical name Docker stored in RepoTags (set in run()) against the
                # archive's RepoTags — both come from Docker so no prefix rules needed.
                tags_match = "name" not in name or (
                    [name["canonical_joined"]] == archived_repo_tags
                )
                if id_matches and tags_match:
                    matches.append((ai, ni))

        matched_archive_idxs = {ai for ai, _ in matches}
        for ai, archived_image in enumerate(archived_images):
            if ai not in matched_archive_idxs:
                archived_repo_tags = archived_image.repo_tags or []
                return f"Overwriting archive since it contains unexpected image {archived_image.image_id} named {', '.join(archived_repo_tags)}"

        matched_name_idxs = {ni for _, ni in matches}
        missing = [self.names[ni] for ni in range(len(self.names)) if ni not in matched_name_idxs]
        if missing:
            return f"Overwriting archive since it is missing image(s) {', '.join([name['joined'] for name in missing])}"

        return None

    def write_chunks(self, chunks: t.Generator[bytes]) -> None:
        try:
            with open(self.path, "wb") as fd:
                for chunk in chunks:
                    fd.write(chunk)
        except Exception as exc:  # pylint: disable=broad-exception-caught
            self.fail(f"Error writing image archive {self.path} - {exc}")

    def _platform_param(self) -> str:
        platform = _Platform.parse_platform_string(self.platform)
        platform_spec: dict[str, str] = {}
        if platform.os:
            platform_spec["os"] = platform.os
        if platform.arch:
            platform_spec["architecture"] = platform.arch
        if platform.variant:
            platform_spec["variant"] = platform.variant
        return json.dumps(platform_spec)

    def export_images(self) -> None:
        image_names = [name["joined"] for name in self.names]
        image_names_str = ", ".join(image_names)
        if len(image_names) == 1:
            self.log(f"Getting archive of image {image_names[0]}")
            params: dict[str, t.Any] = {}
            if self.platform:
                params["platform"] = self._platform_param()
            try:
                chunks = self.client._stream_raw_result(
                    self.client._get(
                        self.client._url("/images/{0}/get", image_names[0]),
                        stream=True,
                        params=params,
                    ),
                    chunk_size=DEFAULT_DATA_CHUNK_SIZE,
                    decode=False,
                )
            except Exception as exc:  # pylint: disable=broad-exception-caught
                self.fail(f"Error getting image {image_names[0]} - {exc}")
        else:
            self.log(f"Getting archive of images {image_names_str}")
            params = {"names": image_names}
            if self.platform:
                params["platform"] = self._platform_param()
            try:
                chunks = self.client._stream_raw_result(
                    self.client._get(
                        self.client._url("/images/get"),
                        stream=True,
                        params=params,
                    ),
                    chunk_size=DEFAULT_DATA_CHUNK_SIZE,
                    decode=False,
                )
            except Exception as exc:  # pylint: disable=broad-exception-caught
                self.fail(f"Error getting images {image_names_str} - {exc}")

        self.write_chunks(chunks)

    def run(self) -> dict[str, t.Any]:
        tag = self.tag
        if not tag:
            tag = "latest"

        images = []
        for name in self.names:
            if "id" in name:
                image = self.client.find_image_by_id(
                    name["id"], accept_missing_image=True
                )
            else:
                image = self.client.find_image(name=name["name"], tag=name["tag"])
            if not image:
                self.fail(f"Image {name['joined']} not found")
            images.append(image)

            # Will have a 'sha256:' prefix
            name["id"] = image["Id"]

            # Determine the canonical name Docker will store in the archive's RepoTags.
            # Docker normalises names at pull time (e.g. docker.io/library/busybox →
            # busybox), so we read it back from RepoTags rather than guessing the rules.
            if "name" in name:
                canonical = _canonical_name_from_repo_tags(
                    image.get("RepoTags") or [], name["tag"]
                )
                name["canonical_joined"] = canonical if canonical is not None else name["joined"]

        results = {
            "changed": False,
            "images": images,
        }

        reason = self.get_export_reason()
        if reason is not None:
            results["msg"] = reason
            results["changed"] = True

            if not self.check_mode:
                self.export_images()

        return results


def main() -> None:
    argument_spec = {
        "path": {"type": "path"},
        "force": {"type": "bool", "default": False},
        "names": {
            "type": "list",
            "elements": "str",
            "required": True,
            "aliases": ["name"],
        },
        "tag": {"type": "str", "default": "latest"},
        "platform": {"type": "str"},
    }

    option_minimal_versions = {
        "platform": {"docker_api_version": "1.48"},
    }

    client = AnsibleDockerClient(
        argument_spec=argument_spec,
        supports_check_mode=True,
        option_minimal_versions=option_minimal_versions,
    )

    try:
        results = ImageExportManager(client).run()
        client.module.exit_json(**results)
    except DockerException as e:
        client.fail(
            f"An unexpected Docker error occurred: {e}",
            exception=traceback.format_exc(),
        )
    except RequestException as e:
        client.fail(
            f"An unexpected requests error occurred when trying to talk to the Docker daemon: {e}",
            exception=traceback.format_exc(),
        )


if __name__ == "__main__":
    main()
