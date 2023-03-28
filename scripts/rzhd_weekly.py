import os
import sys
from rzhd import Rzhd


class RzhdWeekly(Rzhd):

    @staticmethod
    def shift_columns(df):
        """
        Shift of a columns.
        """
        for i, r in df.iterrows():
            if df.loc[i]["container_prefix"].isdigit():
                df.loc[i][16:] = df.loc[i][16:].shift(1)


if __name__ == "__main__":
    rzhd: RzhdWeekly = RzhdWeekly(os.path.abspath(sys.argv[1]), sys.argv[2])
    rzhd.main()
