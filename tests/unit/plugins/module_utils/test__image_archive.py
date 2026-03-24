# Copyright 2022 Red Hat | Ansible
# GNU General Public License v3.0+ (see LICENSES/GPL-3.0-or-later.txt or https://www.gnu.org/licenses/gpl-3.0.txt)
# SPDX-License-Identifier: GPL-3.0-or-later

from __future__ import annotations

import tarfile
import typing as t

import pytest

from ansible_collections.community.docker.plugins.module_utils._image_archive import (
    ImageArchiveInvalidException,
    api_image_id,
    archived_image_manifest,
    load_archived_image_manifest,
)

from ..test_support.docker_image_archive_stubbing import (
    write_imitation_archive,
    write_imitation_archive_with_manifest,
    write_imitation_oci_archive,
    write_irrelevant_tar,
)


@pytest.fixture(name="tar_file_name")
def tar_file_name_fixture(tmpdir: t.Any) -> str:
    """
    Return the name of a non-existing tar file in an existing temporary directory.
    """

    # Cast to str required by Python 2.x
    return str(tmpdir.join("foo.tar"))


@pytest.mark.parametrize(
    "expected, value", [("sha256:foo", "foo"), ("sha256:bar", "bar")]
)
def test_api_image_id_from_archive_id(expected: str, value: str) -> None:
    assert api_image_id(value) == expected


def test_archived_image_manifest_extracts(tar_file_name: str) -> None:
    expected_id = "abcde12345"
    expected_tags = ["foo:latest", "bar:v1"]

    write_imitation_archive(tar_file_name, expected_id, expected_tags)

    actual = archived_image_manifest(tar_file_name)

    assert actual is not None
    assert actual.image_id == expected_id
    assert actual.repo_tags == expected_tags


def test_archived_image_manifest_extracts_nothing_when_file_not_present(
    tar_file_name: str,
) -> None:
    image_id = archived_image_manifest(tar_file_name)

    assert image_id is None


def test_archived_image_manifest_raises_when_file_not_a_tar() -> None:
    try:
        archived_image_manifest(__file__)
        raise AssertionError()
    except ImageArchiveInvalidException as e:
        assert isinstance(e.__cause__, tarfile.ReadError)
        assert str(__file__) in str(e)


def test_archived_image_manifest_raises_when_tar_missing_manifest(
    tar_file_name: str,
) -> None:
    write_irrelevant_tar(tar_file_name)

    try:
        archived_image_manifest(tar_file_name)
        raise AssertionError()
    except ImageArchiveInvalidException as e:
        assert isinstance(e.__cause__, KeyError)
        assert "manifest.json" in str(e.__cause__)


def test_archived_image_manifest_raises_when_manifest_missing_id(
    tar_file_name: str,
) -> None:
    manifest = [{"foo": "bar"}]

    write_imitation_archive_with_manifest(tar_file_name, manifest)

    try:
        archived_image_manifest(tar_file_name)
        raise AssertionError()
    except ImageArchiveInvalidException as e:
        assert isinstance(e.__cause__, KeyError)
        assert "Config" in str(e.__cause__)


def test_load_archived_manifest_populates_manifest_id_from_oci_index(
    tar_file_name: str,
) -> None:
    config_hash = "bf756fb1ae65adf866bd8c456593cd24beb6a0a061dedf42b26a993176745f6b"
    manifest_hash = "90659bf80b44ce6be8234e6ff90a1ac34acbeb826903b02cfa0da11c82cbc042"

    write_imitation_oci_archive(tar_file_name, config_hash, manifest_hash, ["hello-world:latest"])

    results = load_archived_image_manifest(tar_file_name)

    assert results is not None
    assert len(results) == 1
    assert results[0].image_id == config_hash
    assert results[0].manifest_id == manifest_hash


def test_load_archived_manifest_manifest_id_is_none_without_oci(
    tar_file_name: str,
) -> None:
    write_imitation_archive(tar_file_name, "abcde12345", ["foo:latest"])

    results = load_archived_image_manifest(tar_file_name)

    assert results is not None
    assert len(results) == 1
    assert results[0].manifest_id is None


def test_load_archived_manifest_populates_platform_from_config_blob(
    tar_file_name: str,
) -> None:
    config_hash = "bf756fb1ae65adf866bd8c456593cd24beb6a0a061dedf42b26a993176745f6b"
    manifest_hash = "90659bf80b44ce6be8234e6ff90a1ac34acbeb826903b02cfa0da11c82cbc042"

    write_imitation_oci_archive(
        tar_file_name, config_hash, manifest_hash, ["busybox:latest"],
        architecture="amd64", os="linux",
    )

    results = load_archived_image_manifest(tar_file_name)

    assert results is not None
    assert len(results) == 1
    assert results[0].platform == "linux/amd64"


def test_load_archived_manifest_populates_platform_with_variant(
    tar_file_name: str,
) -> None:
    config_hash = "aaaa1111" * 8
    manifest_hash = "bbbb2222" * 8

    write_imitation_oci_archive(
        tar_file_name, config_hash, manifest_hash, ["myimage:arm"],
        architecture="arm64", os="linux", variant="v8",
    )

    results = load_archived_image_manifest(tar_file_name)

    assert results is not None
    assert len(results) == 1
    assert results[0].platform == "linux/arm64/v8"


def test_load_archived_manifest_platform_is_none_without_oci(
    tar_file_name: str,
) -> None:
    write_imitation_archive(tar_file_name, "abcde12345", ["foo:latest"])

    results = load_archived_image_manifest(tar_file_name)

    assert results is not None
    assert len(results) == 1
    assert results[0].platform is None


def test_load_archived_manifest_platform_is_none_when_config_missing_fields(
    tar_file_name: str,
) -> None:
    config_hash = "cccc3333" * 8
    manifest_hash = "dddd4444" * 8

    # No architecture/os kwargs → config blob not written → platform stays None
    write_imitation_oci_archive(tar_file_name, config_hash, manifest_hash, ["scratch:latest"])

    results = load_archived_image_manifest(tar_file_name)

    assert results is not None
    assert len(results) == 1
    assert results[0].platform is None
