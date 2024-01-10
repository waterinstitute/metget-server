#!/usr/bin/env python3
###################################################################################################
# MIT License
#
# Copyright (c) 2023 The Water Institute
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#
# Author: Zach Cobell
# Contact: zcobell@thewaterinstitute.org
# Organization: The Water Institute
#
###################################################################################################


def generate_default_date_range():
    from datetime import datetime, timedelta

    start = datetime.utcnow()
    start = datetime(start.year, start.month, start.day, 0, 0, 0) - timedelta(days=1)
    end = start + timedelta(days=2)
    return start, end


def nam_download():
    import logging

    from metgetlib.ncepnamdownloader import NcepNamdownloader

    logger = logging.getLogger(__name__)

    start, end = generate_default_date_range()
    nam = NcepNamdownloader(start, end)
    logger.info(
        f"Beginning to run NCEP-NAM from {start.isoformat():s} to {end.isoformat():s}"
    )
    n = nam.download()
    logger.info("NCEP-NAM complete. " + str(n) + " files downloaded")
    return n


def gfs_download():
    import logging

    from metgetlib.ncepgfsdownloader import NcepGfsdownloader

    logger = logging.getLogger(__name__)

    start, end = generate_default_date_range()
    gfs = NcepGfsdownloader(start, end)
    logger.info(
        f"Beginning to run NCEP-GFS from {start.isoformat():s} to {end.isoformat():s}"
    )
    n = gfs.download()
    logger.info(f"NCEP-GFS complete. {n:d} files downloaded")
    return n


def gefs_download():
    import logging

    from metgetlib.ncepgefsdownloader import NcepGefsdownloader

    logger = logging.getLogger(__name__)

    start, end = generate_default_date_range()
    gefs = NcepGefsdownloader(start, end)
    logger.info(
        f"Beginning to run NCEP-GEFS from {start.isoformat():s} to {end.isoformat():s}"
    )
    n = gefs.download()
    logger.info(f"NCEP-GEFS complete. {n:d} files downloaded")
    return n


def hwrf_download():
    import logging

    from metgetlib.hwrfdownloader import HwrfDownloader

    logger = logging.getLogger(__name__)

    start, end = generate_default_date_range()
    hwrf = HwrfDownloader(start, end)
    logger.info(
        f"Beginning to run HWRF from {start.isoformat():s} to {end.isoformat():s}"
    )
    n = hwrf.download()
    logger.info(f"HWRF complete. {n:d} files downloaded")
    return n


def hafs_download():
    import logging

    from metbuild.metfiletype import NCEP_HAFS_A
    from metgetlib.hafsdownloader import HafsDownloader

    logger = logging.getLogger(__name__)

    start, end = generate_default_date_range()
    hafs = HafsDownloader(start, end, NCEP_HAFS_A)
    logger.info(
        f"Beginning to run HAFS from {start.isoformat():s} to {end.isoformat():s}"
    )
    n = hafs.download()
    logger.info(f"HAFS complete. {n:d} files downloaded")
    return n


def nhc_download():
    import logging

    from metgetlib.nhcdownloader import NhcDownloader

    logger = logging.getLogger(__name__)

    nhc = NhcDownloader()
    logger.info("Beginning downloading NHC data")
    n = nhc.download()
    logger.info(f"NHC complete. {n:d} files downloaded")
    return n


def coamps_download():
    import logging

    from metgetlib.coampsdownloader import CoampsDownloader

    logger = logging.getLogger(__name__)

    coamps = CoampsDownloader()
    logger.info("Beginning downloading COAMPS data")
    n = coamps.download()
    logger.info(f"COAMPS complete. {n:d} files downloaded")
    return n


def ctcx_download():
    import logging

    from metgetlib.ctcxdownloader import CtcxDownloader

    logger = logging.getLogger(__name__)

    ctcx = CtcxDownloader()
    n = ctcx.download()
    logger.info(f"CTCX complete. {n:d} files downloaded")
    return n


def hrrr_download():
    import logging

    from metgetlib.ncephrrrdownloader import NcepHrrrdownloader

    logger = logging.getLogger(__name__)

    start, end = generate_default_date_range()
    hrrr = NcepHrrrdownloader(start, end)
    logger.info("Beginning downloading HRRR data")
    n = hrrr.download()

    logger.info(f"HRRR complete. {n:d} files downloaded")
    return n


def hrrr_alaska_download():
    import logging

    from metgetlib.ncephrrralaskadownloader import NcepHrrrAlaskadownloader

    logger = logging.getLogger(__name__)

    start, end = generate_default_date_range()
    hrrr = NcepHrrrAlaskadownloader(start, end)
    logger.info("Beginning downloading HRRR-Alaska data")
    n = hrrr.download()
    logger.info(f"HRRR complete. {n:d} files downloaded")
    return n


def wpc_download():
    import logging

    from metgetlib.wpcdownloader import WpcDownloader

    logger = logging.getLogger(__name__)

    start, end = generate_default_date_range()
    wpc = WpcDownloader(start, end)
    logger.info("Beginning downloading WPC data")
    n = wpc.download()
    logger.info(f"WPC complete. {n:d} files downloaded")
    return n


def main():
    import argparse
    import logging

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s :: %(levelname)s :: %(module)s :: %(message)s",
    )
    logger = logging.getLogger(__name__)

    p = argparse.ArgumentParser(description="MetGet Download Function")
    p.add_argument(
        "--service",
        type=str,
        required=True,
        help="Service to download from (nam, gfs, gefs, hwrf, nhc, coamps, hrrr, hrrr-alaska, wpc)",
    )
    args = p.parse_args()

    logger.info(f"Running with configuration: {args.service:s}")

    if args.service == "nam":
        nam_download()
    elif args.service == "gfs":
        gfs_download()
    elif args.service == "gefs":
        gefs_download()
    elif args.service == "hwrf":
        hwrf_download()
    elif args.service == "hafs":
        hafs_download()
    elif args.service == "nhc":
        nhc_download()
    elif args.service == "coamps":
        coamps_download()
    elif args.service == "ctcx":
        ctcx_download()
    elif args.service == "hrrr":
        hrrr_download()
    elif args.service == "hrrr-alaska":
        hrrr_alaska_download()
    elif args.service == "wpc":
        wpc_download()
    else:
        msg = "Invalid data source selected"
        raise RuntimeError(msg)


if __name__ == "__main__":
    main()
