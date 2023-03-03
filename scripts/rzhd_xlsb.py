from rzhd import *


class RZHDXLSB(RZHD):
    pass


if __name__ == "__main__":
    rzhd_xlsb: RZHDXLSB = RZHDXLSB(os.path.abspath(sys.argv[1]), sys.argv[2])
    rzhd_xlsb.main("pyxlsb")
