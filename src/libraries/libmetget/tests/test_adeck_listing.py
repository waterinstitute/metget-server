###################################################################################################
# A-deck discovery must cover every published basin, not just the Atlantic.
#
# NHC aid_public only lists AL/EP/CP. UCAR adecks_open lists those plus WP/IO/SH (and LS when
# present). MetGet unions the two listings so GFS AVNO positions exist for vortex removal in
# every basin. Directory parsing is tested against snippets of the real index HTML; the
# downloader is tested with listings mocked so it is not Atlantic-only by construction.
###################################################################################################
from unittest.mock import MagicMock, patch

from libmetget.download.adeck import ADeckDownloaderException, ADeckStorms
from libmetget.download.adeckdownloader import ADeckDownloader

NHC_INDEX = """
Index of /atcf/aid_public
aal012026.dat.gz        2026-06-18 02:48   67K
aep092026.dat.gz        2026-08-26 14:34  381K
acp012026.dat.gz        2026-08-26 14:40  2.2M
"""

UCAR_INDEX = """
Index of /repository/data/adecks_open
| | aal012026.dat | 2026-08-26 15:19 | 955K |
| | aep092026.dat | 2026-08-26 15:19 | 4.6M |
| | acp012026.dat | 2026-08-26 15:19 | 30M |
| | awp122026.dat | 2026-08-26 15:19 | 5.8M |
| | aio902026.dat | 2026-08-26 15:19 | 726K |
| | ash312026.dat | 2026-08-26 15:19 | 5.7M |
| | ash982027.dat | 2026-08-26 15:19 | 588K |
"""


def test_storms_from_index_keeps_all_basins_and_drops_other_years() -> None:
    nhc = ADeckStorms._storms_from_index(NHC_INDEX, 2026)
    assert nhc == {("AL", 1), ("EP", 9), ("CP", 1)}

    ucar = ADeckStorms._storms_from_index(UCAR_INDEX, 2026)
    assert ("WP", 12) in ucar
    assert ("IO", 90) in ucar
    assert ("SH", 31) in ucar
    # UCAR's flat directory can contain the next Southern Hemisphere season.
    assert ("SH", 98) not in ucar


def test_list_available_storms_unions_nhc_and_ucar(monkeypatch) -> None:
    def fake_get(url, timeout=30):
        response = MagicMock()
        response.status_code = 200
        if "aid_public" in url:
            response.text = NHC_INDEX
        else:
            response.text = UCAR_INDEX
        return response

    monkeypatch.setattr("libmetget.download.adeck.requests.get", fake_get)

    storms = ADeckStorms.list_available_storms(2026)
    basins = {basin for basin, _storm in storms}
    assert basins == {"AL", "EP", "CP", "WP", "IO", "SH"}
    assert ("SH", 31) in storms
    assert ("WP", 12) in storms
    assert ("IO", 90) in storms


def test_list_available_storms_still_covers_jtwc_if_nhc_listing_fails(
    monkeypatch,
) -> None:
    def fake_get(url, timeout=30):
        response = MagicMock()
        if "aid_public" in url:
            response.status_code = 500
            response.text = ""
        else:
            response.status_code = 200
            response.text = UCAR_INDEX
        return response

    monkeypatch.setattr("libmetget.download.adeck.requests.get", fake_get)

    storms = ADeckStorms.list_available_storms(2026)
    basins = {basin for basin, _storm in storms}
    # UCAR mirrors NHC basins too, so a failed NHC listing must not drop EP/CP/AL
    # and must still include JTWC basins.
    assert basins == {"AL", "EP", "CP", "WP", "IO", "SH"}


class _FakeQuery:
    def filter(self, *args, **kwargs):
        return self

    def all(self):
        return []

    def count(self):
        return 0


class _FakeSession:
    def query(self, *args, **kwargs):
        return _FakeQuery()

    def add(self, *args, **kwargs):
        return None

    def commit(self):
        return None


class _FakeDatabase:
    def session(self):
        return _FakeSession()


def test_downloader_fetches_every_discovered_basin(monkeypatch) -> None:
    discovered = [
        ("AL", 1),
        ("EP", 9),
        ("CP", 1),
        ("WP", 12),
        ("IO", 90),
        ("SH", 31),
    ]
    requested = []

    monkeypatch.setattr(
        "libmetget.download.adeckdownloader.Database", _FakeDatabase
    )
    monkeypatch.setattr(
        ADeckStorms, "list_available_storms", staticmethod(lambda year: discovered)
    )

    def fake_download(self, basin, year, storm):
        requested.append((basin, storm))
        raise ADeckDownloaderException("recorded")

    monkeypatch.setattr(ADeckStorms, "download_storm", fake_download)

    with patch.object(ADeckDownloader, "get_model_names", return_value=None):
        downloader = ADeckDownloader()
        count = downloader.download(2026)

    assert count == 0
    assert requested == discovered
    assert {basin for basin, _storm in requested} == {
        "AL",
        "EP",
        "CP",
        "WP",
        "IO",
        "SH",
    }
