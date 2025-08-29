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
import argparse
import logging
from datetime import datetime, timezone

from libmetget.download.adeckdownloader import ADeckDownloader
from libmetget.download.coampsdownloader import CoampsDownloader
from libmetget.download.ctcxdownloader import CtcxDownloader
from libmetget.download.hafsdownloader import HafsDownloader
from libmetget.download.hwrfdownloader import HwrfDownloader
from libmetget.download.ncepgefsdownloader import NcepGefsdownloader
from libmetget.download.ncepgfsdownloader import NcepGfsdownloader
from libmetget.download.ncephrrralaskadownloader import NcepHrrrAlaskadownloader
from libmetget.download.ncephrrrdownloader import NcepHrrrdownloader
from libmetget.download.ncepnamdownloader import NcepNamdownloader
from libmetget.download.nceprefsdownloader import NcepRefsDownloader
from libmetget.download.nceprrfsdownloader import NcepRrfsDownloader
from libmetget.download.nhcdownloader import NhcDownloader
from libmetget.download.wpcdownloader import WpcDownloader
from libmetget.sources.metfiletype import NCEP_HAFS_A, NCEP_HAFS_B
from libmetget.version import get_metget_version

logger = logging.getLogger(__name__)


def generate_default_date_range():
    from datetime import datetime, timedelta, timezone

    start = datetime.now(timezone.utc)
    start = datetime(start.year, start.month, start.day, 0, 0, 0) - timedelta(days=1)
    end = start + timedelta(days=2)
    return start, end


def nam_download():
    start, end = generate_default_date_range()
    nam = NcepNamdownloader(start, end)
    logger.info(
        f"Beginning to run NCEP-NAM from {start.isoformat():s} to {end.isoformat():s}"
    )
    n = nam.download()
    logger.info("NCEP-NAM complete. " + str(n) + " files downloaded")
    return n


def gfs_download():
    start, end = generate_default_date_range()
    gfs = NcepGfsdownloader(start, end)
    logger.info(
        f"Beginning to run NCEP-GFS from {start.isoformat():s} to {end.isoformat():s}"
    )
    n = gfs.download()
    logger.info(f"NCEP-GFS complete. {n:d} files downloaded")
    return n


def gefs_download():
    start, end = generate_default_date_range()
    gefs = NcepGefsdownloader(start, end)
    logger.info(
        f"Beginning to run NCEP-GEFS from {start.isoformat():s} to {end.isoformat():s}"
    )
    n = gefs.download()
    logger.info(f"NCEP-GEFS complete. {n:d} files downloaded")
    return n


def hwrf_download():
    start, end = generate_default_date_range()
    hwrf = HwrfDownloader(start, end)
    logger.info(
        f"Beginning to run HWRF from {start.isoformat():s} to {end.isoformat():s}"
    )
    n = hwrf.download()
    logger.info(f"HWRF complete. {n:d} files downloaded")
    return n


def hafs_download():
    start, end = generate_default_date_range()
    hafs_a = HafsDownloader(start, end, NCEP_HAFS_A)
    hafs_b = HafsDownloader(start, end, NCEP_HAFS_B)

    logger.info(
        f"Beginning to run HAFS-A from {start.isoformat():s} to {end.isoformat():s}"
    )
    n_hafs_a = hafs_a.download()
    logger.info(f"HAFS A complete. {n_hafs_a:d} files downloaded")

    logger.info(
        f"Beginning to run HAFS-B from {start.isoformat():s} to {end.isoformat():s}"
    )
    n_hafs_b = hafs_b.download()
    logger.info(f"HAFS B complete. {n_hafs_b:d} files downloaded")

    n = n_hafs_a + n_hafs_b
    logger.info(f"HAFS A and B complete. {n:d} files downloaded")

    return n


def nhc_download():
    nhc = NhcDownloader()
    logger.info("Beginning downloading NHC data")
    n = nhc.download()
    logger.info(f"NHC complete. {n:d} files downloaded")
    return n


def coamps_download():
    coamps = CoampsDownloader()
    logger.info("Beginning downloading COAMPS data")
    n = coamps.download()
    logger.info(f"COAMPS complete. {n:d} files downloaded")
    return n


def ctcx_download():
    ctcx = CtcxDownloader()
    n = ctcx.download()
    logger.info(f"CTCX complete. {n:d} files downloaded")
    return n


def hrrr_download():
    start, end = generate_default_date_range()
    hrrr = NcepHrrrdownloader(start, end)
    logger.info("Beginning downloading HRRR data")
    n = hrrr.download()

    logger.info(f"HRRR complete. {n:d} files downloaded")
    return n


def hrrr_alaska_download():
    start, end = generate_default_date_range()
    hrrr = NcepHrrrAlaskadownloader(start, end)
    logger.info("Beginning downloading HRRR-Alaska data")
    n = hrrr.download()
    logger.info(f"HRRR complete. {n:d} files downloaded")
    return n


def rrfs_download():
    start, end = generate_default_date_range()
    rrfs = NcepRrfsDownloader(start, end)
    logger.info("Beginning downloading RRFS data")
    n = rrfs.download()
    logger.info(f"RRFS complete. {n:d} files downloaded")
    return n


def refs_download():
    start, end = generate_default_date_range()
    refs = NcepRefsDownloader(start, end)
    logger.info("Beginning downloading REFS data")
    n = refs.download()
    logger.info(f"REFS complete. {n:d} files downloaded")
    return n


def wpc_download():
    logger.info("Beginning downloading WPC data")
    n = WpcDownloader.download()
    logger.info(f"WPC complete. {n:d} files downloaded")
    return n


def adeck_download() -> int:
    a_deck = ADeckDownloader()
    current_year = datetime.now(tz=timezone.utc).year
    n = a_deck.download(current_year)

    logger.info(f"A-Deck complete. {n:d} files downloaded")

    return n


def metget_download():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s :: %(levelname)s :: %(module)s :: %(message)s",
    )

    p = argparse.ArgumentParser(description="MetGet Download Function")
    p.add_argument(
        "--service",
        type=str,
        required=True,
        help="Service to download from (nam, gfs, gefs, hwrf, nhc, coamps, hrrr, hrrr-alaska, wpc, rrfs, refs, adeck)",
    )
    args = p.parse_args()

    logger.info(f"Running MetGet-Server Version: {get_metget_version():s}")

    logger.info(f"Running with configuration: {args.service:s}")

    download_functions = {
        "nam": nam_download,
        "gfs": gfs_download,
        "gefs": gefs_download,
        "hwrf": hwrf_download,
        "hafs": hafs_download,
        "nhc": nhc_download,
        "coamps": coamps_download,
        "ctcx": ctcx_download,
        "hrrr": hrrr_download,
        "hrrr-alaska": hrrr_alaska_download,
        "wpc": wpc_download,
        "adeck": adeck_download,
        "rrfs": rrfs_download,
        "refs": refs_download,
    }

    if args.service in download_functions:
        download_functions[args.service]()
    else:
        msg = "Invalid data source selected"
        raise RuntimeError(msg)


if __name__ == "__main__":
    metget_download()
