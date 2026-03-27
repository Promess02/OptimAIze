from dataclasses import dataclass


@dataclass
class PricingState:
    product_id: str
    current_price: float
    competitor_price: float
    inventory_level: int
    season: int
    unit_cost: float


class RuleBasedRLPolicy:
    """MVP policy approximating a trained RL action head with discrete actions."""

    ACTIONS = [-0.05, -0.02, 0.0, 0.02, 0.05]

    def choose_action(self, state: PricingState, sandbox) -> tuple[float, dict]:
        best_action = 0.0
        best_reward = float("-inf")
        diagnostics = {}

        for action in self.ACTIONS:
            candidate_price = max(0.01, state.current_price * (1.0 + action))
            predicted_sales = sandbox.predict_sales_volume(
                product_id=state.product_id,
                candidate_price=candidate_price,
                competitor_price=state.competitor_price,
                season=state.season,
                inventory=state.inventory_level,
            )

            realized_sales = min(predicted_sales, float(max(state.inventory_level, 0)))
            stockout_penalty = max(0.0, predicted_sales - realized_sales) * state.current_price * 0.1
            overstock_penalty = max(0.0, state.inventory_level - predicted_sales) * 0.02
            margin = max(0.0, candidate_price - state.unit_cost)
            reward = (margin * realized_sales) - stockout_penalty - overstock_penalty

            diagnostics[action] = {
                "candidate_price": round(candidate_price, 2),
                "predicted_sales": round(predicted_sales, 2),
                "reward": round(reward, 2),
            }

            if reward > best_reward:
                best_reward = reward
                best_action = action

        return best_action, {
            "best_reward": round(best_reward, 2),
            "actions": diagnostics,
        }
