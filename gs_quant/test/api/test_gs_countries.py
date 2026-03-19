"""
Copyright 2024 Goldman Sachs.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
"""

import asyncio
from unittest.mock import MagicMock, patch, AsyncMock

import pytest

from gs_quant.api.gs.countries import GsCountryApi
from gs_quant.target.countries import Country, Subdivision


# ===========================================================================
# Helper to create mock session
# ===========================================================================

def _mock_session():
    session = MagicMock()
    return session


# ===========================================================================
# Tests for Country operations
# ===========================================================================

class TestGsCountryApiCountries:
    def test_get_many_countries(self):
        mock_session = _mock_session()
        country1 = Country(name='United States', id_='US')
        country2 = Country(name='United Kingdom', id_='UK')
        mock_session.sync.get.return_value = {'results': (country1, country2)}
        with patch('gs_quant.api.gs.countries.GsSession') as mock_gs:
            mock_gs.current = mock_session
            result = GsCountryApi.get_many_countries(limit=50)
            mock_session.sync.get.assert_called_once_with(
                '/countries?limit=50', cls=Country
            )
            assert result == (country1, country2)

    def test_get_many_countries_default_limit(self):
        mock_session = _mock_session()
        mock_session.sync.get.return_value = {'results': ()}
        with patch('gs_quant.api.gs.countries.GsSession') as mock_gs:
            mock_gs.current = mock_session
            GsCountryApi.get_many_countries()
            mock_session.sync.get.assert_called_once_with(
                '/countries?limit=100', cls=Country
            )

    def test_get_many_countries_async(self):
        mock_session = _mock_session()
        country = Country(name='Germany', id_='DE')
        mock_response = {'results': (country,)}

        async def mock_get(*args, **kwargs):
            return mock_response

        mock_session.async_ = MagicMock()
        mock_session.async_.get = mock_get

        with patch('gs_quant.api.gs.countries.GsSession') as mock_gs:
            mock_gs.current = mock_session
            loop = asyncio.new_event_loop()
            try:
                result = loop.run_until_complete(GsCountryApi.get_many_countries_async(limit=50))
                assert result == (country,)
            finally:
                loop.close()

    def test_get_many_countries_async_default_limit(self):
        mock_session = _mock_session()
        mock_response = {'results': ()}

        async def mock_get(*args, **kwargs):
            return mock_response

        mock_session.async_ = MagicMock()
        mock_session.async_.get = mock_get

        with patch('gs_quant.api.gs.countries.GsSession') as mock_gs:
            mock_gs.current = mock_session
            loop = asyncio.new_event_loop()
            try:
                result = loop.run_until_complete(GsCountryApi.get_many_countries_async())
                assert result == ()
            finally:
                loop.close()

    def test_get_country(self):
        mock_session = _mock_session()
        country = Country(name='France', id_='FR')
        mock_session.sync.get.return_value = country
        with patch('gs_quant.api.gs.countries.GsSession') as mock_gs:
            mock_gs.current = mock_session
            result = GsCountryApi.get_country('FR')
            mock_session.sync.get.assert_called_once_with('/countries/FR', cls=Country)
            assert result == country

    def test_get_country_async(self):
        mock_session = _mock_session()
        country = Country(name='Japan', id_='JP')

        async def mock_get(*args, **kwargs):
            return country

        mock_session.async_ = MagicMock()
        mock_session.async_.get = mock_get

        with patch('gs_quant.api.gs.countries.GsSession') as mock_gs:
            mock_gs.current = mock_session
            loop = asyncio.new_event_loop()
            try:
                result = loop.run_until_complete(GsCountryApi.get_country_async('JP'))
                assert result == country
            finally:
                loop.close()

    def test_create_country(self):
        mock_session = _mock_session()
        country = Country(name='Canada', id_='CA')
        mock_session.sync.post.return_value = country
        with patch('gs_quant.api.gs.countries.GsSession') as mock_gs:
            mock_gs.current = mock_session
            result = GsCountryApi.create_country(country)
            mock_session.sync.post.assert_called_once_with('/countries', country, cls=Country)
            assert result == country

    def test_update_country(self):
        mock_session = _mock_session()
        country = Country(name='Australia', id_='AU')
        mock_session.sync.put.return_value = country
        with patch('gs_quant.api.gs.countries.GsSession') as mock_gs:
            mock_gs.current = mock_session
            result = GsCountryApi.update_country(country)
            mock_session.sync.put.assert_called_once_with('/countries/AU', country, cls=Country)
            assert result == country

    def test_delete_country(self):
        mock_session = _mock_session()
        mock_session.sync.delete.return_value = {'status': 'ok'}
        with patch('gs_quant.api.gs.countries.GsSession') as mock_gs:
            mock_gs.current = mock_session
            result = GsCountryApi.delete_country('FR')
            mock_session.sync.delete.assert_called_once_with('/countries/FR')
            assert result == {'status': 'ok'}


# ===========================================================================
# Tests for Subdivision operations
# ===========================================================================

class TestGsCountryApiSubdivisions:
    def test_get_many_subdivisions(self):
        mock_session = _mock_session()
        sub1 = Subdivision(name='California', id_='US-CA', country_id='US')
        sub2 = Subdivision(name='Texas', id_='US-TX', country_id='US')
        mock_session.sync.get.return_value = {'results': (sub1, sub2)}
        with patch('gs_quant.api.gs.countries.GsSession') as mock_gs:
            mock_gs.current = mock_session
            result = GsCountryApi.get_many_subdivisions(limit=50)
            mock_session.sync.get.assert_called_once_with(
                '/countries/subdivisions?limit=50', cls=Subdivision
            )
            assert result == (sub1, sub2)

    def test_get_many_subdivisions_default_limit(self):
        mock_session = _mock_session()
        mock_session.sync.get.return_value = {'results': ()}
        with patch('gs_quant.api.gs.countries.GsSession') as mock_gs:
            mock_gs.current = mock_session
            GsCountryApi.get_many_subdivisions()
            mock_session.sync.get.assert_called_once_with(
                '/countries/subdivisions?limit=100', cls=Subdivision
            )

    def test_get_subdivision(self):
        mock_session = _mock_session()
        sub = Subdivision(name='Ontario', id_='CA-ON', country_id='CA')
        mock_session.sync.get.return_value = sub
        with patch('gs_quant.api.gs.countries.GsSession') as mock_gs:
            mock_gs.current = mock_session
            result = GsCountryApi.get_subdivision('CA-ON')
            mock_session.sync.get.assert_called_once_with(
                '/countries/subdivisions/CA-ON', cls=Subdivision
            )
            assert result == sub

    def test_create_subdivision(self):
        mock_session = _mock_session()
        sub = Subdivision(name='Bavaria', id_='DE-BY', country_id='DE')
        mock_session.sync.post.return_value = sub
        with patch('gs_quant.api.gs.countries.GsSession') as mock_gs:
            mock_gs.current = mock_session
            result = GsCountryApi.create_subdivision(sub)
            mock_session.sync.post.assert_called_once_with(
                '/countries/subdivisions', sub, cls=Subdivision
            )
            assert result == sub

    def test_update_subdivision(self):
        mock_session = _mock_session()
        sub = Subdivision(name='Bavaria Updated', id_='DE-BY', country_id='DE')
        mock_session.sync.put.return_value = sub
        with patch('gs_quant.api.gs.countries.GsSession') as mock_gs:
            mock_gs.current = mock_session
            result = GsCountryApi.update_subdivision(sub)
            mock_session.sync.put.assert_called_once_with(
                '/countries/subdivisions/DE-BY', sub, cls=Subdivision
            )
            assert result == sub

    def test_delete_subdivision(self):
        mock_session = _mock_session()
        mock_session.sync.delete.return_value = {'status': 'deleted'}
        with patch('gs_quant.api.gs.countries.GsSession') as mock_gs:
            mock_gs.current = mock_session
            result = GsCountryApi.delete_subdivision('US-CA')
            mock_session.sync.delete.assert_called_once_with('/countries/subdivisions/US-CA')
            assert result == {'status': 'deleted'}
