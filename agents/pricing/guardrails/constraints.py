from dataclasses import dataclass


@dataclass
class PricingConstraints:
    min_margin_pct: float = 0.10
    max_competitor_gap_pct: float = 0.20
    max_step_pct: float = 0.15

    def apply(
        self,
        candidate_price: float,
        current_price: float,
        unit_cost: float,
        competitor_price: float,
    ) -> tuple[float, list[str]]:
        violations: list[str] = []

        floor_price = max(unit_cost * (1.0 + self.min_margin_pct), 0.01)
        if candidate_price < floor_price:
            candidate_price = floor_price
            violations.append("floor_margin")

        if competitor_price and competitor_price > 0:
            ceiling_price = competitor_price * (1.0 + self.max_competitor_gap_pct)
            if candidate_price > ceiling_price:
                candidate_price = ceiling_price
                violations.append("competitor_cap")

        if current_price > 0:
            max_up = current_price * (1.0 + self.max_step_pct)
            max_down = current_price * (1.0 - self.max_step_pct)
            if candidate_price > max_up:
                candidate_price = max_up
                violations.append("step_up_cap")
            if candidate_price < max_down:
                candidate_price = max_down
                violations.append("step_down_cap")

        return round(float(candidate_price), 2), violations
