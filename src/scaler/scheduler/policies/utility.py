from typing import Optional, Tuple

from scaler.scheduler.policies.allocation.capability import CapabilityAllocatePolicy
from scaler.scheduler.policies.allocation.even_load import EvenLoadAllocatePolicy
from scaler.scheduler.policies.mixins import ScalingPolicy, TaskAllocatePolicy
from scaler.scheduler.policies.scaling.capability import CapabilityScalingPolicy
from scaler.scheduler.policies.scaling.static import StaticScalingPolicy
from scaler.scheduler.policies.scaling.vanilla import VanillaScalingPolicy
from scaler.scheduler.policies.scaling.waterfall import WaterfallScalingPolicy, parse_waterfall_rules
from scaler.scheduler.policies.types import AllocatePolicyStrategy, PolicyEngineType, ScalingPolicyStrategy

_WATERFALL_V1_ALLOCATE_POLICY = AllocatePolicyStrategy.CAPABILITY


def create_allocate_policy(allocate_policy_strategy: AllocatePolicyStrategy) -> TaskAllocatePolicy:
    if allocate_policy_strategy == AllocatePolicyStrategy.CAPABILITY:
        return CapabilityAllocatePolicy()
    elif allocate_policy_strategy == AllocatePolicyStrategy.EVEN_LOAD:
        return EvenLoadAllocatePolicy()

    raise ValueError(f"unsupported allocate policy strategy: {allocate_policy_strategy}")


def create_scaling_policy(scaling: str) -> ScalingPolicy:
    """Build a scaling policy from its `name[:argument]` string. Only `static` takes an argument."""
    name, _, argument = scaling.partition(":")
    scaling_policy_strategy = ScalingPolicyStrategy(name)

    if scaling_policy_strategy == ScalingPolicyStrategy.STATIC:
        return StaticScalingPolicy(_parse_static_task_concurrency(argument))

    if argument:
        raise ValueError(f"scaling policy {name!r} takes no argument, got {scaling!r}")

    if scaling_policy_strategy == ScalingPolicyStrategy.VANILLA:
        return VanillaScalingPolicy()
    elif scaling_policy_strategy == ScalingPolicyStrategy.CAPABILITY:
        return CapabilityScalingPolicy()

    raise ValueError(f"unsupported scaling policy strategy: {scaling_policy_strategy}")


def _parse_static_task_concurrency(argument: str) -> Optional[int]:
    if not argument:
        return None

    if not argument.isdigit():
        raise ValueError(f"static scaling takes a non-negative task concurrency, got {argument!r}")

    return int(argument)


def create_policies(policy_engine_type: str, policy_content: str) -> Tuple[TaskAllocatePolicy, ScalingPolicy]:
    engine_type = PolicyEngineType(policy_engine_type)

    if engine_type == PolicyEngineType.SIMPLE:
        policy_kv = {
            k.strip(): v.strip() for item in policy_content.split(";") if "=" in item for k, v in [item.split("=", 1)]
        }

        required_keys = {"allocate", "scaling"}
        if policy_kv.keys() != required_keys:
            raise ValueError(f"simple policy_content requires keys {required_keys}, got {set(policy_kv.keys())}")

        return (
            create_allocate_policy(AllocatePolicyStrategy(policy_kv["allocate"])),
            create_scaling_policy(policy_kv["scaling"]),
        )

    if engine_type == PolicyEngineType.WATERFALL_V1:
        rules = parse_waterfall_rules(policy_content)
        return create_allocate_policy(_WATERFALL_V1_ALLOCATE_POLICY), WaterfallScalingPolicy(rules)

    raise ValueError(f"Unknown policy_engine_type: {policy_engine_type}")
