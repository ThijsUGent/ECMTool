def energy_convert(value, unit, elec=False):
    """
    Converts energy or emission values to more readable units with 2 significant figures.

    Parameters:
        value (float): The input value.
        unit (str): Input unit. Can be 'GJ', 't', or 'kt'.
        elec (bool): If True, energy is converted to MWh/TWh.

    Returns:
        (converted_value, new_unit): Tuple with converted float and unit.
    """
    def round_sf(x, sf=2):
        """Round x to sf significant figures."""
        if x == 0:
            return 0
        from math import log10, floor
        return round(x, -int(floor(log10(abs(x)))) + (sf - 1))

    # Handle emissions (convert to tonnes first)
    if unit == "kt":
        if value >= 1_000:
            return round_sf(value / 1_000_000), "Mt"
        else:
            return round_sf(value), "kt"

    if unit == "t":
        if value >= 1_000_000:
            return round_sf(value / 1_000_000), "Mt"
        elif value >= 1_000:
            return round_sf(value / 1_000), "kt"
        else:
            return round_sf(value), "t"

    # Handle electricity
    if elec:
        # 1 GJ = 0.277778 MWh
        value_mwh = value * 0.277778
        if value_mwh >= 1_000_000:
            return round_sf(value_mwh / 1_000_000), "TWh"
        else:
            return round_sf(value_mwh), "MWh"

    # Handle generic energy conversion from GJ
    if unit == "GJ":
        if value >= 1_000_000:
            return round_sf(value / 1_000_000), "PJ"
        elif value >= 1_000:
            return round_sf(value / 1_000), "TJ"
        else:
            return round_sf(value), "GJ"

    # Fallback if unknown unit
    return round_sf(value), unit