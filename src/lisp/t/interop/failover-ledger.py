"""Client observations for a manual failover rehearsal."""


class CommitLedger:
    """Keep independent client outcomes until the promoted server can be read."""

    def __init__(self):
        self.outcomes = {}

    def record(self, witness, outcome):
        if witness in self.outcomes:
            raise ValueError(f"reused failover witness {witness}")
        if outcome not in {"acknowledged", "rejected", "unknown"}:
            raise ValueError(f"invalid client outcome {outcome}")
        self.outcomes[witness] = outcome

    def compare(self, rows):
        # An ACK records a client observation, not a synchronous durability
        # guarantee. Unknown means that client never observed a COMMIT response.
        observed = set(rows)
        if len(observed) != len(rows):
            raise AssertionError("promoted server contains duplicate witnesses")
        unexpected = observed.difference(self.outcomes)
        if unexpected:
            raise AssertionError(f"unrecorded witnesses on promoted server: {sorted(unexpected)}")
        result = {outcome: {"present": [], "absent": []}
                  for outcome in ("acknowledged", "rejected", "unknown")}
        for witness, outcome in sorted(self.outcomes.items()):
            result[outcome]["present" if witness in observed else "absent"].append(witness)
        if result["rejected"]["present"]:
            raise AssertionError(f"rejected transactions became visible: {result['rejected']['present']}")
        return result
