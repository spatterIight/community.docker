# Copyright 2022 Red Hat | Ansible
# GNU General Public License v3.0+ (see LICENSES/GPL-3.0-or-later.txt or https://www.gnu.org/licenses/gpl-3.0.txt)
# SPDX-License-Identifier: GPL-3.0-or-later

from __future__ import annotations

import json
import tarfile
import typing as t
from io import BytesIO


def _add_bytes(tf: tarfile.TarFile, name: str, data: bytes) -> None:
    buf = BytesIO(data)
    ti = tarfile.TarInfo(name)
    ti.size = len(data)
    tf.addfile(ti, buf)


def write_imitation_archive(
    file_name: str, image_id: str, repo_tags: list[str]
) -> None:
    """
    Write a tar file meeting these requirements:

    * Has a file manifest.json
    * manifest.json contains a one-element array
    * The element has a Config property with "[image_id].json" as the value name

    :param file_name: Name of file to create
    :type file_name: str
    :param image_id: Fake sha256 hash (without the sha256: prefix)
    :type image_id: str
    :param repo_tags: list of fake image tags
    :type repo_tags: list
    """

    manifest = [{"Config": f"{image_id}.json", "RepoTags": repo_tags}]

    write_imitation_archive_with_manifest(file_name, manifest)


def write_imitation_archive_with_manifest(
    file_name: str, manifest: list[dict[str, t.Any]]
) -> None:
    with tarfile.open(file_name, "w") as tf:
        _add_bytes(tf, "manifest.json", json.dumps(manifest).encode("utf-8"))


def write_imitation_oci_archive(
    file_name: str,
    config_hash: str,
    manifest_hash: str,
    repo_tags: list[str],
) -> None:
    """
    Write a Docker 25+/29-style OCI archive containing manifest.json, index.json,
    and the OCI manifest blob under blobs/sha256/.

    :param file_name:      Name of tar file to create
    :param config_hash:    Fake config blob SHA256 hash (without sha256: prefix)
    :param manifest_hash:  Fake OCI manifest SHA256 hash (without sha256: prefix)
    :param repo_tags:      List of fake image tags
    """
    manifest_entry = [{"Config": f"blobs/sha256/{config_hash}", "RepoTags": repo_tags}]
    oci_manifest = {
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.manifest.v1+json",
        "config": {
            "mediaType": "application/vnd.oci.image.config.v1+json",
            "digest": f"sha256:{config_hash}",
            "size": 0,
        },
        "layers": [],
    }
    oci_manifest_bytes = json.dumps(oci_manifest).encode("utf-8")
    index = {
        "schemaVersion": 2,
        "manifests": [
            {
                "mediaType": "application/vnd.oci.image.manifest.v1+json",
                "digest": f"sha256:{manifest_hash}",
                "size": len(oci_manifest_bytes),
                "annotations": {
                    "io.containerd.image.name": repo_tags[0] if repo_tags else "",
                },
            }
        ],
    }
    with tarfile.open(file_name, "w") as tf:
        _add_bytes(tf, "manifest.json", json.dumps(manifest_entry).encode("utf-8"))
        _add_bytes(tf, f"blobs/sha256/{manifest_hash}", oci_manifest_bytes)
        _add_bytes(tf, "index.json", json.dumps(index).encode("utf-8"))


def write_irrelevant_tar(file_name: str) -> None:
    """
    Create a tar file that does not match the spec for "docker image save" / "docker image load" commands.

    :param file_name: Name of tar file to create
    :type file_name: str
    """

    with tarfile.open(file_name, "w") as tf:
        _add_bytes(tf, "hi.txt", "Hello, world.".encode("utf-8"))
