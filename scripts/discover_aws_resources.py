#!/usr/bin/env python3
"""
Discover existing AWS resources for ECS deployment
This script helps find existing VPCs, subnets, security groups, and IAM roles
"""

import os
import sys
from dotenv import load_dotenv

load_dotenv()

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    import boto3
    from botocore.exceptions import ClientError
except ImportError:
    print("Error: boto3 not installed. Run: pip install boto3")
    sys.exit(1)


def get_aws_session():
    """Create AWS session"""
    access_key = os.getenv("AWS_ACCESS_KEY_ID") or os.getenv("ACCESS_KEY_ID_AWS")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY") or os.getenv("SECRET_ACCESS_KEY_AWS")
    region = os.getenv("AWS_REGION", "us-east-2")

    if not access_key or not secret_key:
        print("Error: AWS credentials not found in environment variables")
        sys.exit(1)

    return boto3.Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region
    )


def discover_vpcs(ec2_client):
    """Discover available VPCs"""
    print("\n" + "="*60)
    print("AVAILABLE VPCs")
    print("="*60)

    try:
        response = ec2_client.describe_vpcs()

        if not response["Vpcs"]:
            print("No VPCs found")
            return []

        vpcs = []
        for vpc in response["Vpcs"]:
            vpc_id = vpc["VpcId"]
            cidr = vpc["CidrBlock"]
            is_default = vpc.get("IsDefault", False)
            tags = {tag["Key"]: tag["Value"] for tag in vpc.get("Tags", [])}
            name = tags.get("Name", "N/A")

            print(f"\nVPC ID: {vpc_id}")
            print(f"  Name: {name}")
            print(f"  CIDR: {cidr}")
            print(f"  Default: {is_default}")

            vpcs.append(vpc_id)

        return vpcs
    except ClientError as e:
        print(f"Error discovering VPCs: {e}")
        return []


def discover_subnets(ec2_client, vpc_id=None):
    """Discover available subnets"""
    print("\n" + "="*60)
    print("AVAILABLE SUBNETS" + (f" (VPC: {vpc_id})" if vpc_id else ""))
    print("="*60)

    try:
        filters = [{"Name": "vpc-id", "Values": [vpc_id]}] if vpc_id else []
        response = ec2_client.describe_subnets(Filters=filters)

        if not response["Subnets"]:
            print("No subnets found")
            return []

        # Group by VPC and find pairs in different AZs
        vpcs = {}
        for subnet in response["Subnets"]:
            vpc = subnet["VpcId"]
            if vpc not in vpcs:
                vpcs[vpc] = []
            vpcs[vpc].append(subnet)

        recommendations = []

        for vpc, subnets in vpcs.items():
            print(f"\nVPC: {vpc}")

            # Group by AZ
            az_groups = {}
            for subnet in subnets:
                az = subnet["AvailabilityZone"]
                if az not in az_groups:
                    az_groups[az] = []
                az_groups[az].append(subnet)

            # Show subnets grouped by AZ
            for az, az_subnets in az_groups.items():
                print(f"\n  Availability Zone: {az}")
                for subnet in az_subnets:
                    subnet_id = subnet["SubnetId"]
                    cidr = subnet["CidrBlock"]
                    public = subnet.get("MapPublicIpOnLaunch", False)
                    tags = {tag["Key"]: tag["Value"] for tag in subnet.get("Tags", [])}
                    name = tags.get("Name", "N/A")

                    print(f"    Subnet ID: {subnet_id}")
                    print(f"      Name: {name}")
                    print(f"      CIDR: {cidr}")
                    print(f"      Public: {public}")

            # Recommend pairs of subnets in different AZs
            if len(az_groups) >= 2:
                azs = list(az_groups.keys())[:2]
                subnet_pair = [az_groups[azs[0]][0]["SubnetId"], az_groups[azs[1]][0]["SubnetId"]]
                recommendations.append((vpc, subnet_pair))

        return recommendations
    except ClientError as e:
        print(f"Error discovering subnets: {e}")
        return []


def discover_security_groups(ec2_client, vpc_id=None):
    """Discover available security groups"""
    print("\n" + "="*60)
    print("AVAILABLE SECURITY GROUPS" + (f" (VPC: {vpc_id})" if vpc_id else ""))
    print("="*60)

    try:
        filters = [{"Name": "vpc-id", "Values": [vpc_id]}] if vpc_id else []
        response = ec2_client.describe_security_groups(Filters=filters)

        if not response["SecurityGroups"]:
            print("No security groups found")
            return []

        security_groups = []
        for sg in response["SecurityGroups"]:
            sg_id = sg["GroupId"]
            sg_name = sg["GroupName"]
            vpc = sg["VpcId"]
            description = sg["Description"]

            print(f"\nSecurity Group ID: {sg_id}")
            print(f"  Name: {sg_name}")
            print(f"  VPC: {vpc}")
            print(f"  Description: {description}")

            # Check if it allows port 8080
            allows_8080 = False
            for rule in sg.get("IpPermissions", []):
                from_port = rule.get("FromPort")
                to_port = rule.get("ToPort")
                if from_port and to_port and from_port <= 8080 <= to_port:
                    allows_8080 = True
                    break

            print(f"  Allows port 8080: {allows_8080}")

            security_groups.append((sg_id, vpc, allows_8080))

        return security_groups
    except ClientError as e:
        print(f"Error discovering security groups: {e}")
        return []


def discover_iam_roles(iam_client):
    """Discover ECS-related IAM roles"""
    print("\n" + "="*60)
    print("ECS-RELATED IAM ROLES")
    print("="*60)

    try:
        response = iam_client.list_roles()

        ecs_roles = []
        for role in response["Roles"]:
            role_name = role["RoleName"]
            role_arn = role["Arn"]

            # Check if it's ECS-related
            if "ecs" in role_name.lower() or "ECS" in role_name:
                print(f"\nRole Name: {role_name}")
                print(f"  ARN: {role_arn}")

                # Check attached policies
                try:
                    policies = iam_client.list_attached_role_policies(RoleName=role_name)
                    if policies["AttachedPolicies"]:
                        print("  Attached Policies:")
                        for policy in policies["AttachedPolicies"]:
                            print(f"    - {policy['PolicyName']}")

                            # Check if it's a task execution role
                            if "TaskExecution" in policy["PolicyName"]:
                                ecs_roles.append((role_arn, "execution"))
                            elif "Task" in policy["PolicyName"]:
                                ecs_roles.append((role_arn, "task"))
                except Exception:
                    pass

        if not ecs_roles:
            print("\nNo ECS-related roles found")
            print("You may need to create one or request permissions")

        return ecs_roles
    except ClientError as e:
        print(f"Error discovering IAM roles: {e}")
        return []


def generate_env_config(subnet_recommendations, security_groups, iam_roles):
    """Generate .env configuration"""
    print("\n" + "="*60)
    print("RECOMMENDED CONFIGURATION")
    print("="*60)

    if not subnet_recommendations:
        print("\nNo suitable subnet pairs found.")
        print("You need at least 2 subnets in different availability zones.")
        return

    print("\nAdd these lines to your .env file:")
    print()

    # Recommend first VPC with subnet pair
    vpc_id, subnet_pair = subnet_recommendations[0]
    print(f"# Using VPC: {vpc_id}")
    print(f"DE_BENCH_ECS_SUBNET_IDS={','.join(subnet_pair)}")

    # Recommend a security group from the same VPC
    sg_for_vpc = [sg for sg, vpc, _ in security_groups if vpc == vpc_id]
    if sg_for_vpc:
        print(f"DE_BENCH_ECS_SECURITY_GROUP_IDS={sg_for_vpc[0]}")
    else:
        print("# No security group found for this VPC - you may need to create one")
        print("# DE_BENCH_ECS_SECURITY_GROUP_IDS=sg-xxxxx")

    # Recommend an execution role
    execution_roles = [arn for arn, role_type in iam_roles if role_type == "execution"]
    if execution_roles:
        print(f"DE_BENCH_ECS_TASK_EXECUTION_ROLE_ARN={execution_roles[0]}")
    else:
        print("# No ECS task execution role found - you may need to create one")
        print("# DE_BENCH_ECS_TASK_EXECUTION_ROLE_ARN=arn:aws:iam::123456789:role/ecsTaskExecutionRole")

    print()


def main():
    """Main function"""
    print("AWS Resource Discovery Tool")
    print("Discovering existing AWS resources for ECS deployment...")

    # Get AWS session
    session = get_aws_session()
    ec2_client = session.client("ec2")
    iam_client = session.client("iam")

    # Discover resources
    vpcs = discover_vpcs(ec2_client)

    # Discover subnets (use first VPC if available)
    vpc_id = vpcs[0] if vpcs else None
    subnet_recommendations = discover_subnets(ec2_client, vpc_id)

    # Discover security groups
    security_groups = discover_security_groups(ec2_client, vpc_id)

    # Discover IAM roles
    iam_roles = discover_iam_roles(iam_client)

    # Generate recommended configuration
    generate_env_config(subnet_recommendations, security_groups, iam_roles)

    print("\n" + "="*60)
    print("NOTE: If no suitable resources were found, you have two options:")
    print("1. Request an AWS admin to create the resources")
    print("2. Request IAM permissions to create resources automatically")
    print("="*60 + "\n")


if __name__ == "__main__":
    main()
