#!/usr/bin/env python3
"""
Diagnose ECS task connectivity issues
Checks security groups, network configuration, and task status
"""

import os
import sys
from dotenv import load_dotenv

load_dotenv()

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import boto3
from botocore.exceptions import ClientError


def get_aws_clients():
    """Get AWS clients"""
    access_key = os.getenv("AWS_ACCESS_KEY_ID") or os.getenv("ACCESS_KEY_ID_AWS")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY") or os.getenv("SECRET_ACCESS_KEY_AWS")
    region = os.getenv("AWS_REGION", "us-east-2")

    session = boto3.Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region
    )

    return {
        "ecs": session.client("ecs"),
        "ec2": session.client("ec2"),
        "logs": session.client("logs")
    }


def get_task_details(ecs_client, cluster_name, task_arn):
    """Get detailed task information"""
    response = ecs_client.describe_tasks(
        cluster=cluster_name,
        tasks=[task_arn]
    )

    if not response["tasks"]:
        print(f"❌ Task not found: {task_arn}")
        return None

    return response["tasks"][0]


def check_task_status(task):
    """Check task running status"""
    print("\n" + "="*60)
    print("TASK STATUS")
    print("="*60)

    last_status = task["lastStatus"]
    desired_status = task["desiredStatus"]

    print(f"Last Status: {last_status}")
    print(f"Desired Status: {desired_status}")

    if last_status != "RUNNING":
        print(f"❌ Task is not running yet (status: {last_status})")
        print("   Wait for task to reach RUNNING state")
        return False

    print("✅ Task is RUNNING")

    # Check containers
    print("\nContainer Status:")
    for container in task.get("containers", []):
        name = container["name"]
        status = container.get("lastStatus", "UNKNOWN")
        health = container.get("healthStatus", "N/A")
        print(f"  - {name}: {status} (health: {health})")

        if status != "RUNNING":
            print(f"    ❌ Container {name} is not running")
            return False

    return True


def get_task_network_info(task):
    """Extract network information from task"""
    network_info = {
        "eni_id": None,
        "public_ip": None,
        "private_ip": None,
        "subnet_id": None,
        "vpc_id": None,
        "security_groups": []
    }

    # Get ENI details
    for attachment in task.get("attachments", []):
        if attachment["type"] == "ElasticNetworkInterface":
            for detail in attachment.get("details", []):
                if detail["name"] == "networkInterfaceId":
                    network_info["eni_id"] = detail["value"]
                elif detail["name"] == "subnetId":
                    network_info["subnet_id"] = detail["value"]

    return network_info


def check_network_interface(ec2_client, eni_id):
    """Check network interface details"""
    print("\n" + "="*60)
    print("NETWORK INTERFACE")
    print("="*60)

    try:
        response = ec2_client.describe_network_interfaces(
            NetworkInterfaceIds=[eni_id]
        )

        if not response["NetworkInterfaces"]:
            print(f"❌ Network interface not found: {eni_id}")
            return None

        eni = response["NetworkInterfaces"][0]

        # Public IP
        association = eni.get("Association", {})
        public_ip = association.get("PublicIp")
        private_ip = eni.get("PrivateIpAddress")

        print(f"ENI ID: {eni_id}")
        print(f"Private IP: {private_ip}")
        print(f"Public IP: {public_ip or '❌ NO PUBLIC IP'}")

        if not public_ip:
            print("\n❌ PROBLEM: No public IP assigned!")
            print("   This usually means:")
            print("   - Subnet doesn't have auto-assign public IP enabled")
            print("   - OR assignPublicIp is not set to ENABLED in task config")
            return None

        # Security groups
        security_groups = [sg["GroupId"] for sg in eni.get("Groups", [])]
        print(f"Security Groups: {', '.join(security_groups)}")

        # VPC and Subnet
        vpc_id = eni.get("VpcId")
        subnet_id = eni.get("SubnetId")
        print(f"VPC ID: {vpc_id}")
        print(f"Subnet ID: {subnet_id}")

        return {
            "public_ip": public_ip,
            "private_ip": private_ip,
            "security_groups": security_groups,
            "vpc_id": vpc_id,
            "subnet_id": subnet_id
        }

    except ClientError as e:
        print(f"❌ Error getting network interface: {e}")
        return None


def check_security_groups(ec2_client, security_group_ids, port=8080):
    """Check if security groups allow traffic on specified port"""
    print("\n" + "="*60)
    print(f"SECURITY GROUP RULES (Port {port})")
    print("="*60)

    try:
        response = ec2_client.describe_security_groups(
            GroupIds=security_group_ids
        )

        allows_port = False

        for sg in response["SecurityGroups"]:
            sg_id = sg["GroupId"]
            sg_name = sg["GroupName"]

            print(f"\nSecurity Group: {sg_name} ({sg_id})")
            print("Inbound Rules:")

            if not sg.get("IpPermissions"):
                print("  ❌ No inbound rules!")
                continue

            for rule in sg["IpPermissions"]:
                protocol = rule.get("IpProtocol", "")
                from_port = rule.get("FromPort", "Any")
                to_port = rule.get("ToPort", "Any")

                # Check if rule allows our port
                if protocol == "-1":  # All traffic
                    allows_port = True
                    print(f"  ✅ ALL TRAFFIC allowed")
                elif from_port <= port <= to_port:
                    allows_port = True
                    sources = []
                    for ip_range in rule.get("IpRanges", []):
                        sources.append(ip_range["CidrIp"])
                    print(f"  ✅ Port {port} allowed from: {', '.join(sources)}")
                else:
                    sources = []
                    for ip_range in rule.get("IpRanges", []):
                        sources.append(ip_range["CidrIp"])
                    if sources:
                        print(f"  - Ports {from_port}-{to_port} from: {', '.join(sources)}")

        if not allows_port:
            print(f"\n❌ PROBLEM: Port {port} is NOT allowed in security groups!")
            print(f"   Add inbound rule: TCP port {port} from 0.0.0.0/0")
            return False

        print(f"\n✅ Port {port} is allowed")
        return True

    except ClientError as e:
        print(f"❌ Error checking security groups: {e}")
        return False


def check_subnet_routes(ec2_client, subnet_id):
    """Check if subnet has route to internet gateway"""
    print("\n" + "="*60)
    print("SUBNET ROUTING")
    print("="*60)

    try:
        # Get subnet details
        subnet_response = ec2_client.describe_subnets(SubnetIds=[subnet_id])
        subnet = subnet_response["Subnets"][0]
        vpc_id = subnet["VpcId"]

        print(f"Subnet ID: {subnet_id}")
        print(f"VPC ID: {vpc_id}")
        print(f"Auto-assign Public IP: {subnet.get('MapPublicIpOnLaunch', False)}")

        # Get route tables
        rt_response = ec2_client.describe_route_tables(
            Filters=[
                {"Name": "association.subnet-id", "Values": [subnet_id]}
            ]
        )

        if not rt_response["RouteTables"]:
            # Check VPC main route table
            rt_response = ec2_client.describe_route_tables(
                Filters=[
                    {"Name": "vpc-id", "Values": [vpc_id]},
                    {"Name": "association.main", "Values": ["true"]}
                ]
            )

        has_igw_route = False

        for rt in rt_response["RouteTables"]:
            print(f"\nRoute Table: {rt['RouteTableId']}")
            print("Routes:")

            for route in rt.get("Routes", []):
                destination = route.get("DestinationCidrBlock", "N/A")
                target = route.get("GatewayId", route.get("NatGatewayId", "N/A"))

                print(f"  - {destination} → {target}")

                if destination == "0.0.0.0/0" and target.startswith("igw-"):
                    has_igw_route = True
                    print(f"    ✅ Internet Gateway route found")

        if not has_igw_route:
            print("\n❌ PROBLEM: No route to Internet Gateway!")
            print("   Add route: 0.0.0.0/0 → igw-xxxxx")
            return False

        print("\n✅ Subnet has route to Internet Gateway")
        return True

    except ClientError as e:
        print(f"❌ Error checking subnet routes: {e}")
        return False


def check_container_logs(logs_client, log_group, log_stream_prefix, namespace):
    """Check container logs for errors"""
    print("\n" + "="*60)
    print("CONTAINER LOGS (Last 20 lines)")
    print("="*60)

    try:
        # List log streams
        log_streams = logs_client.describe_log_streams(
            logGroupName=log_group,
            logStreamNamePrefix=log_stream_prefix,
            orderBy="LastEventTime",
            descending=True,
            limit=1
        )

        if not log_streams.get("logStreams"):
            print(f"❌ No log streams found for {log_group}/{log_stream_prefix}")
            print("   Container may not have started yet")
            return

        log_stream_name = log_streams["logStreams"][0]["logStreamName"]

        # Get log events
        events_response = logs_client.get_log_events(
            logGroupName=log_group,
            logStreamName=log_stream_name,
            limit=20,
            startFromHead=False
        )

        events = events_response.get("events", [])

        if not events:
            print("No log events found")
            return

        for event in events:
            message = event["message"].strip()
            print(message)

        # Check for common issues
        all_logs = "\n".join([e["message"] for e in events])

        if "error" in all_logs.lower() or "exception" in all_logs.lower():
            print("\n⚠️  Errors found in logs - check above")

        if "listening" in all_logs.lower() or "started" in all_logs.lower():
            print("\n✅ Container appears to have started successfully")
        else:
            print("\n⚠️  Container may still be starting up")

    except logs_client.exceptions.ResourceNotFoundException:
        print(f"❌ Log group {log_group} not found")
        print("   Logs may not be configured or container hasn't started")
    except ClientError as e:
        print(f"❌ Error getting logs: {e}")


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Diagnose ECS task connectivity")
    parser.add_argument("namespace", help="Task namespace/name (e.g., ecs-testing)")
    parser.add_argument("--cluster", default=None, help="ECS cluster name")

    args = parser.parse_args()

    # Get cluster name
    cluster_name = args.cluster or os.getenv("DE_BENCH_ECS_CLUSTER_NAME")
    if not cluster_name:
        print("❌ Cluster name not provided and DE_BENCH_ECS_CLUSTER_NAME not set")
        sys.exit(1)

    print("="*60)
    print("ECS TASK CONNECTIVITY DIAGNOSTIC")
    print("="*60)
    print(f"Namespace: {args.namespace}")
    print(f"Cluster: {cluster_name}")

    # Get AWS clients
    clients = get_aws_clients()

    # Find task
    print("\nFinding task...")
    try:
        tasks = clients["ecs"].list_tasks(
            cluster=cluster_name,
            desiredStatus="RUNNING"
        )

        # Get all task details
        if not tasks.get("taskArns"):
            print(f"❌ No running tasks found in cluster {cluster_name}")
            sys.exit(1)

        all_tasks = clients["ecs"].describe_tasks(
            cluster=cluster_name,
            tasks=tasks["taskArns"]
        )

        # Find task matching namespace
        matching_task = None
        for task in all_tasks["tasks"]:
            task_def_arn = task["taskDefinitionArn"]
            if args.namespace in task_def_arn:
                matching_task = task
                break

        if not matching_task:
            print(f"❌ No task found matching namespace: {args.namespace}")
            print(f"Available tasks:")
            for task in all_tasks["tasks"]:
                print(f"  - {task['taskDefinitionArn']}")
            sys.exit(1)

        task_arn = matching_task["taskArn"]
        print(f"✅ Found task: {task_arn}")

        # Run diagnostics
        issues_found = []

        # 1. Check task status
        if not check_task_status(matching_task):
            issues_found.append("Task is not running")

        # 2. Get network info
        network_info = get_task_network_info(matching_task)

        if not network_info["eni_id"]:
            print("\n❌ Could not find network interface")
            issues_found.append("No network interface")
        else:
            # 3. Check network interface
            eni_details = check_network_interface(clients["ec2"], network_info["eni_id"])

            if not eni_details:
                issues_found.append("Network interface issues")
            else:
                # 4. Check security groups
                if not check_security_groups(clients["ec2"], eni_details["security_groups"]):
                    issues_found.append("Security group doesn't allow port 8080")

                # 5. Check subnet routing
                if not check_subnet_routes(clients["ec2"], eni_details["subnet_id"]):
                    issues_found.append("No route to Internet Gateway")

        # 6. Check container logs
        log_group = f"/ecs/{args.namespace}"
        check_container_logs(clients["logs"], log_group, "airflow", args.namespace)

        # Summary
        print("\n" + "="*60)
        print("DIAGNOSTIC SUMMARY")
        print("="*60)

        if issues_found:
            print("❌ Issues found:")
            for issue in issues_found:
                print(f"  - {issue}")
            print("\nFix these issues to enable connectivity")
        else:
            print("✅ No obvious connectivity issues found")
            print("\nIf still not accessible, check:")
            print("  - Container is actually listening on port 8080")
            print("  - Container has finished starting up")
            print("  - Application isn't crashing on startup")

        if eni_details and eni_details.get("public_ip"):
            print(f"\nTry accessing: http://{eni_details['public_ip']}:8080")

    except ClientError as e:
        print(f"❌ Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
