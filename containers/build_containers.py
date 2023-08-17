#!/usr/bin/env python3
import logging
from typing import Tuple

CONTAINERS = [
    "database",
    "download",
    "build",
    "api",
    "keymanager",
]


def find_root_directory() -> str:
    """
    Find the root directory of the repository

    Returns:
        The root directory of the repository
    """
    import os

    current_directory = os.getcwd()
    while True:
        if os.path.exists(os.path.join(current_directory, ".git")):
            return current_directory
        else:
            current_directory = os.path.dirname(current_directory)


def check_for_docker() -> None:
    """
    Check the process (ps) list to see if docker is running
    """
    import subprocess

    log = logging.getLogger(__name__)

    os_type = get_os_type()

    if os_type == "linux":
        cmd = ["ps", "-ef"]

        status = subprocess.run(cmd, capture_output=True)
        if status.returncode != 0:
            raise RuntimeError("Failed to get process list")

        if "dockerd" not in status.stdout.decode("utf-8"):
            log.error("Docker daemon is not running")
            raise RuntimeError("Docker daemon is not running")
    elif os_type == "macos":
        cmd = ["docker", "info"]

        status = subprocess.run(cmd, capture_output=True)
        if status.returncode != 0:
            raise RuntimeError("Failed to get docker info")

        if "Docker Desktop" not in status.stdout.decode("utf-8"):
            log.error("Docker daemon is not running")
            raise RuntimeError("Docker daemon is not running")


def get_os_type() -> str:
    """
    Get the type of operating system. Currently only supports
    linux and MacOS, so raise an error otherwise

    Returns:
        The type of operating system
    """
    from sys import platform

    if platform == "linux" or platform == "linux2":
        return "linux"
    elif platform == "darwin":
        return "macos"
    else:
        raise RuntimeError("Unsupported OS: {:s}".format(platform))


def generate_container_name(container: str, repo: str, tag: str) -> Tuple[str, str]:
    """
    Generate the name of the container and the path to the Dockerfile

    Args:
        container: Name of the container
        repo: Name of the repository where the image will be pushed
        tag: Tag to use for the container

    Returns:
        The name of the container and the path to the Dockerfile
    """
    import os

    container_path = os.path.join("containers", container, "Dockerfile")
    container_name = "{:s}/metget-{:s}:{:s}".format(repo, container, tag)
    return container_name, container_path


def build_container(
    container: str, repo: str, tag: str, alias: list, verbose: bool
) -> None:
    """
    Build a container with the given tag

    Args:
        container: Name of the container to build
        repo: Name of the container repository
        tag: Tag to use for the container
        alias: Additional tags to apply
        verbose: show docker logs

    Returns:
        None
    """
    import subprocess

    log = logging.getLogger(__name__)

    container_name, container_path = generate_container_name(container, repo, tag)

    log.info(
        "Begin building container {:s} at path {:s}".format(
            container_name, container_path
        )
    )

    cmd = [
        "docker",
        "buildx",
        "build",
        "--platform",
        "linux/amd64",
        "-f",
        container_path,
        "-t",
        container_name,
        ".",
    ]

    if verbose:
        log.info("Begin Docker log")
    status = subprocess.run(cmd, capture_output=(not verbose))

    # ...Check if the command failed
    if status.returncode != 0:
        log.error("Failed to build container")
        print(status.stdout.decode("utf-8"))
        print(status.stderr.decode("utf-8"))
        raise RuntimeError("Failed to build container")

    if verbose:
        log.info("End Docker log")

    for t in alias:
        tag_image(container, repo, tag, t)


def tag_image(container: str, repo: str, current_tag: str, new_tag: str) -> None:
    """
    Tag a container with the given tag

    Args:
        container: Name of the container to tag
        repo: Name of the repository where the image is pushed
        current_tag: Current tag of the container
        new_tag: New tag to apply to the container

    Returns:
        None
    """
    import subprocess

    log = logging.getLogger(__name__)

    current_image, _ = generate_container_name(container, repo, current_tag)
    new_image, _ = generate_container_name(container, repo, new_tag)
    log.info("Tagging {:s} as {:s}".format(current_image, new_image))

    cmd = ["docker", "tag", current_image, new_image]

    subprocess.run(cmd, capture_output=False)


def push_container_tags(
    container: str,
    repo: str,
    tag: str,
    alias: list,
    verbose: bool,
) -> None:
    """
    Push the main container and associated tags to the repo

    Args:
        container: Name of the container to push
        repo: Name of the repository where the image is pushed
        tag: Tag to use for the container
        alias: Additional tags to apply
        verbose: show docker logs
    """
    push_container(container, repo, tag, verbose)
    for t in alias:
        push_container(container, repo, t, verbose)


def push_container(container: str, repo: str, tag: str, verbose: bool) -> None:
    """
    Push a container with the given tag

    Args:
        container: Name of the container to push
        repo: Name of the repository where the image is pushed
        tag: Tag to use for the container
        verbose: show docker logs
    """
    import subprocess

    log = logging.getLogger(__name__)

    # ...First, push the main container
    container_name, _ = generate_container_name(container, repo, tag)
    log.info("Pushing container {:s}".format(container_name))
    cmd = ["docker", "push", container_name]

    if verbose:
        log.info("Begin Docker log")

    status = subprocess.run(cmd, capture_output=(not verbose))

    # ...Check if the command failed
    if status.returncode != 0:
        log.error("Failed to push container")
        print(status.stdout.decode("utf-8"))
        print(status.stderr.decode("utf-8"))
        raise RuntimeError("Failed to push container")

    if verbose:
        log.info("End Docker log")


def main():
    import argparse
    import os

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s :: %(levelname)s :: %(filename)s :: %(funcName)s :: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S%Z",
    )

    parser = argparse.ArgumentParser(
        description="Script to build docker containers for metget-server"
    )
    parser.add_argument(
        "--containers",
        nargs="+",
        help="List of containers to build (defualt builds all)",
        default=["all"],
    )
    parser.add_argument(
        "--tag",
        type=str,
        help="Tag to use for the containers. Auto to attempt automatic detection",
        required=True,
    )
    parser.add_argument(
        "--alias",
        nargs="+",
        help="Additional tags/aliases to apply to the container (i.e. latest, nightly, etc)",
    )
    parser.add_argument("--push", action="store_true", help="Push the containers")
    parser.add_argument(
        "--repo",
        type=str,
        help="Name of the repository where the image will be pushed",
        required=True,
    )
    parser.add_argument("--verbose", action="store_true", help="Verbose output")

    args = parser.parse_args()

    log = logging.getLogger(__name__)

    # ...Check that the docker daemon is running
    check_for_docker()

    # ...The containers are built from the top level
    # repository directory. Change there now
    build_directory = find_root_directory()
    log.info("Changing to directory: {:s}".format(build_directory))
    os.chdir(build_directory)

    if os.getcwd() != build_directory:
        raise RuntimeError("Failed to change to the correct directory")

    if args.containers == ["all"]:
        container_list = CONTAINERS
    else:
        container_list = args.containers

    for container in container_list:
        if container not in CONTAINERS:
            raise ValueError("Unknown container: {:s}".format(container))

        build_container(container, args.repo, args.tag, args.alias, args.verbose)
        if args.push:
            push_container_tags(
                container,
                args.repo,
                args.tag,
                args.alias,
                args.verbose,
            )


if __name__ == "__main__":
    main()
