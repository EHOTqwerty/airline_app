def in_params(prefix: str, values: list[str]):
    """
    Zwraca: (placeholders, params_dict)
    placeholders: ":c0,:c1,:c2"
    params: {"c0": "PL", ...}
    """
    params = {}
    ph = []
    for i, v in enumerate(values):
        k = f"{prefix}{i}"
        params[k] = v
        ph.append(f":{k}")
    return ",".join(ph), params