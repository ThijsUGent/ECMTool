import pandas as pd
import re


def process_configuration_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    def extract_outermost_parentheses(s):
        stack = []
        start = None
        for i, char in enumerate(s):
            if char == "(":
                if not stack:
                    start = i
                stack.append(char)
            elif char == ")":
                stack.pop()
                if not stack:
                    return s[start: i + 1]
        return None

    def extract_fuel(config):
        if not isinstance(config, str):
            return "-"
        if "((" in config:
            return extract_outermost_parentheses(config)
        if ")" in config:
            match = re.search(r"\((.*?)\)", config)
            if match:
                extracted = match.group(1)
            else:
                extracted = None
            return f"({extracted})" if extracted else "-"
        return "-"

    # Extract fuel
    df["energy_feedstock"] = df["configuration_name"].apply(extract_fuel)

    # Technology category logic
    ccs_keywords = ["MEA", "DEA", "-MEA", "-DEA", "CC"]
    df["technology_category"] = "-"
    df.loc[
        df["configuration_name"].str.contains(
            "|".join(ccs_keywords), case=False, na=False
        ),
        "technology_category",
    ] = "CCS"

    # Hydrogen source logic
    df["hydrogen_source"] = "-"
    df.loc[
        df["configuration_name"].str.contains("AEL", case=False, na=False),
        "hydrogen_source",
    ] = "alkaline electrolyser"

    # Electrification logic
    electrification_keywords = ["EAF", r"\bEL\b", "MOE"]
    for keyword in electrification_keywords:
        mask = df["configuration_name"].str.contains(
            keyword, case=False, na=False, regex=True
        )
        df.loc[
            mask & ~df["technology_category"].str.contains(
                "Electrification", na=False),
            "technology_category",
        ] += " Electrification"

    return df
