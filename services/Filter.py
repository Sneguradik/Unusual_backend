import pandas as pd


class Filter:
    def __init__(self, name: str, description: str, condition:str, value: float, type:str, useTrigger:bool, active:bool ):
        self.name = name
        self.description = description
        self.condition = condition
        self.value = value
        self.type = type
        self.useTrigger = useTrigger
        self.active = active

    def apply(self, df:pd.DataFrame) -> pd.DataFrame:
        if not self.active : return df

        if self.type == "And":
            res_df : pd.DataFrame

            match self.condition:
                case "Equals":
                    res_df = df.loc[df[self.name] == self.value]
                case "Less":
                    res_df = df.loc[df[self.name] < self.value]
                case "Greater":
                    res_df = df.loc[df[self.name] > self.value]
                case "LessOrEquals":
                    res_df = df.loc[df[self.name] <= self.value]
                case "GreaterOrEquals":
                    res_df = df.loc[df[self.name] >= self.value]
                case _:
                    res_df = df.loc[df[self.name] == self.value]

            if self.useTrigger:
                if "triggers" in res_df.columns: res_df["triggers"] = 1
                else : res_df["triggers"] +=1

            return res_df

        elif self.type == "Or" and self.useTrigger:

            if "triggers" in df.columns: df["triggers"] = 0

            match self.condition:
                case "Equals":
                    df["triggers"] += int(df[self.name] == self.value)
                case "Less":
                    df["triggers"] += int(df[self.name] < self.value)
                case "Greater":
                    df["triggers"] += int(df[self.name] > self.value)
                case "LessOrEquals":
                    df["triggers"] += int(df[self.name] <= self.value)
                case "GreaterOrEquals":
                    df["triggers"] += int(df[self.name] >= self.value)
                case _:
                    df["triggers"] += int(df[self.name] != self.value)

            return df

        else: return df
