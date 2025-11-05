"""
Kubernetes Manifest Manager - Generate and/or apply Kubernetes manifests
"""

import argparse
import os
import sys
import time

from dotenv import load_dotenv
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from Environment.Kubernetes.ManifestManager import KubernetesManifestManager

load_dotenv()


def profile(number_of_instances: int, container: str):
    """
    Profile the manifest generation and application process

    :param int number_of_instances: Number of instances to create for profiling
    :param str container: The container image to use for odd-numbered namespaces
    """
    namespaces = []
    # generate random names
    for i in range(number_of_instances):
        namespace_name = f"profile-namespace-{i + 1}"
        namespaces.append(namespace_name)
    start_time = time.time()
    k8s_manager = KubernetesManifestManager()
    print(
        f"Initialized KubernetesManifestManager in {time.time() - start_time:.2f} seconds"
    )
    for ns in namespaces:
        # see if the namespace ends with an even number
        ns_split = ns.split("-")
        last_number = int(ns_split[-1])
        if last_number % 2 == 0:
            manifest = k8s_manager.generate_manifest(
                namespace=ns,
                container="airflowstates-cudfbgfvekd7f0at.azurecr.io/hello-world-fail:base",
            )
        else:
            manifest = k8s_manager.generate_manifest(namespace=ns, container=container)
        print(f"Generated manifest in {time.time() - start_time:.2f} seconds")
        file_path = f"{ns}-manifest.yml"
        with open(file_path, "w") as f:
            f.write(manifest)
        print(f"Manifest generated and saved to: {file_path}")
        print(f"Saved manifest in {time.time() - start_time:.2f} seconds")
        k8s_manager.apply_manifest(manifest)
        print(
            f"Manifest applied successfully for namespace: {ns} in {time.time() - start_time:.2f} seconds"
        )
        if external_ip := k8s_manager.get_service_external_ip(namespace=ns):
            print(f"External IP for namespace {ns}: {external_ip}")
            print(
                f"Retrieved external IP in {time.time() - start_time:.2f} seconds"
            )
        else:
            print(f"Failed to retrieve external IP for namespace {ns}")
        # use requests to hit the external IP and port 8080 to verify the pod is running
        if external_ip and k8s_manager.verify_pod_health(external_ip):
            print(f"Pod in namespace {ns} is healthy")
            print(f"Verified pod health in {time.time() - start_time:.2f} seconds")
        else:
            print(
                f"Pod in namespace {ns} is not healthy or external IP not available"
            )
    print(
        f"Total time taken: {time.time() - start_time:.2f} seconds for {len(namespaces)} namespace(s)"
    )
    print(
        f"Time per namespace: {(time.time() - start_time) / len(namespaces):.2f} seconds"
    )



def main():
    """Main entry point for the CLI"""
    parser = argparse.ArgumentParser(
        description="Kubernetes Manifest Manager - Generate and/or apply Kubernetes manifests"
    )

    subparsers = parser.add_subparsers(
        dest="action", help="Action to perform", required=True
    )

    # Generate manifest subcommand
    generate_parser = subparsers.add_parser(
        "generate", help="Generate a manifest and save it to a file"
    )
    generate_parser.add_argument("namespace", type=str, help="The namespace to create")
    generate_parser.add_argument(
        "--container",
        type=str,
        help="The container image to use",
        default="airflowstates-cudfbgfvekd7f0at.azurecr.io/airflow2-session-auth:base",
    )
    generate_parser.add_argument(
        "--output",
        "-o",
        type=str,
        help="Output file path for the manifest",
        required=True,
    )

    # Apply manifest subcommand
    apply_parser = subparsers.add_parser(
        "apply", help="Apply an existing manifest file to the cluster"
    )
    apply_parser.add_argument(
        "manifest_file", type=str, help="Path to the manifest file to apply"
    )

    # Generate and apply manifest subcommand
    generate_apply_parser = subparsers.add_parser(
        "generate-and-apply", help="Generate a manifest and apply it to the cluster"
    )
    generate_apply_parser.add_argument(
        "namespace", type=str, help="The namespace to create"
    )
    generate_apply_parser.add_argument(
        "--container",
        type=str,
        help="The container image to use",
        default="airflowstates-cudfbgfvekd7f0at.azurecr.io/airflow2-session-auth:base",
    )
    generate_apply_parser.add_argument(
        "--output",
        "-o",
        type=str,
        help="Optional: save the generated manifest to this file path",
        default=None,
    )

    # Reapply job subcommand
    reapply_parser = subparsers.add_parser(
        "reapply-job",
        help="Reapply a Job by deleting and recreating it with a new container (handles immutable Job specs)",
    )
    reapply_parser.add_argument(
        "namespace", type=str, help="The namespace where the Job exists"
    )
    reapply_parser.add_argument(
        "container", type=str, help="The new container image to use"
    )

    # Profile subcommand
    profile_parser = subparsers.add_parser(
        "profile", help="Profile the manifest generation and application process"
    )
    profile_parser.add_argument(
        "number_of_instances", 
        type=int, 
        help="Number of instances to create for profiling"
    )
    profile_parser.add_argument(
        "container", 
        type=str, 
        help="The container image to use for odd-numbered namespaces"
    )

    args = parser.parse_args()

    manager = KubernetesManifestManager()

    if args.action == "generate":
        # Generate manifest and save to file
        manifest = manager.generate_manifest(
            namespace=args.namespace, container=args.container
        )
        with open(args.output, "w") as f:
            f.write(manifest)
        print(f"Manifest generated and saved to: {args.output}")

    elif args.action == "apply":
        # Apply existing manifest file
        with open(args.manifest_file, "r") as f:
            manifest = f.read()
        manager.apply_manifest(manifest)
        print(f"\nManifest from {args.manifest_file} applied successfully")

    elif args.action == "generate-and-apply":
        # Generate and apply manifest
        manifest = manager.generate_manifest(
            namespace=args.namespace, container=args.container
        )

        # Optionally save the manifest
        if args.output:
            with open(args.output, "w") as f:
                f.write(manifest)
            print(f"Manifest saved to: {args.output}")

        manager.apply_manifest(manifest)
        print(f"\nManifest applied successfully for namespace: {args.namespace}")

    elif args.action == "reapply-job":
        # Reapply job with new container
        manager.reapply_job(namespace=args.namespace, container=args.container)
        print(f"\nJob reapplied successfully for namespace: {args.namespace}")

    elif args.action == "profile":
        profile(number_of_instances=args.number_of_instances, container=args.container)
        


if __name__ == "__main__":
    main()
