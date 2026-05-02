#!/usr/bin/env python3

import sys

from agents.pricing.guardrails.constraints import PricingConstraints
from agents.pricing.rl_engine.inference import PricingState, RuleBasedRLPolicy


class FakeSandbox:
    def predict_sales_volume(self, product_id, candidate_price, competitor_price, season, inventory):
        base = 100.0
        # demand decreases with price
        demand = base * (competitor_price / max(candidate_price, 1.0))
        if season == 4:
            demand *= 1.1
        return max(0.0, min(demand, inventory * 2))


def assert_true(condition, message):
    if not condition:
        raise AssertionError(message)


def run():
    constraints = PricingConstraints(min_margin_pct=0.1, max_competitor_gap_pct=0.2, max_step_pct=0.10)

    p, violations = constraints.apply(candidate_price=50, current_price=100, unit_cost=95, competitor_price=100)
    assert_true(p >= 104.5, "Price should be clamped above cost floor")
    assert_true("floor_margin" in violations, "Should report floor violation")

    p2, violations2 = constraints.apply(candidate_price=150, current_price=100, unit_cost=60, competitor_price=100)
    assert_true(p2 <= 110, "Step cap should limit extreme increases")
    assert_true("step_up_cap" in violations2 or "competitor_cap" in violations2, "Should apply at least one cap")

    policy = RuleBasedRLPolicy()
    state = PricingState(
        product_id="P001",
        current_price=100,
        competitor_price=100,
        inventory_level=200,
        season=2,
        unit_cost=70,
    )
    action, diag = policy.choose_action(state, FakeSandbox())
    assert_true(action in policy.ACTIONS, "Action must be from discrete action set")
    assert_true("best_reward" in diag, "Diagnostics should include reward")

    print("All pricing RL scenario tests passed.")


if __name__ == "__main__":
    try:
        run()
    except AssertionError as exc:
        print(f"Test failed: {exc}")
        sys.exit(1)
