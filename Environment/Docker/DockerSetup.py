# src/Functions/utility.py
import docker
import os


def load_docker(input_directory):

    client = docker.from_env()

    script_dir = input_directory
    # print(script_dir)
    dockerfile_path = os.path.join(script_dir, "Dockerfile")

    # print(dockerfile_path)
    # Build the Docker image from the script's directory
    image, _ = client.images.build(path=script_dir, dockerfile=dockerfile_path, rm=True)

    # Run the container from the built image
    container = client.containers.run(image, detach=True, tty=True)

    return container
