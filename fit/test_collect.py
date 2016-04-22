#!python
# coding=utf-8

import os
import unittest

import numpy as np
import netCDF4 as nc4

from fit.collect import main

def makedirs(path):
    try:
        os.makedirs(path)
    except FileExistsError:
        pass


class ConverterTests(unittest.TestCase):

    def test_currents(self):
        output = os.path.join(os.path.dirname(__file__), 'output')
        makedirs(output)
        main(output, 'sisp', 'currents')

        for f in os.listdir(output):
            fp = os.path.join(output, f)
            if '_currents_' not in fp:
                continue

            with nc4.Dataset(fp) as nc:

                assert 'latitude' in nc.variables
                assert np.isclose(nc.variables['latitude'][:], 27.862)

                assert 'longitude' in nc.variables
                assert np.isclose(nc.variables['longitude'][:], -80.445)

                assert nc.title == 'SISP'
                assert nc.description == 'Sebastian Inlet State Park, FL - ADCP and weather station'

                assert 'height' in nc.variables

                if 'water_surface_height_above_reference_datum' in fp:
                    assert nc.variables['water_surface_height_above_reference_datum'].discriminant == 'adcp'
                    assert nc.variables['water_surface_height_above_reference_datum'].vertical_datum == 'MLLW'
                
                if 'sea_water_temperature' in fp:
                    assert np.isclose(nc.variables['height'][:], 8.53)
                else:
                    assert nc.variables['height'][:].mask == True


    def test_met(self):
        output = os.path.join(os.path.dirname(__file__), 'output')
        makedirs(output)
        main(output, 'sisp', 'met')

        for f in os.listdir(output):
            fp = os.path.join(output, f)
            if '_mets_' not in fp:
                continue

            with nc4.Dataset(fp) as nc:

                assert 'latitude' in nc.variables
                assert np.isclose(nc.variables['latitude'][:], 27.862)

                assert 'longitude' in nc.variables
                assert np.isclose(nc.variables['longitude'][:], -80.445)

                assert nc.title == 'SISP'
                assert nc.description == 'Sebastian Inlet State Park, FL - ADCP and weather station'

                assert 'height' in nc.variables

                if 'air_temperature' in fp:
                    assert np.isclose(nc.variables['height'][:], -10.09)
                elif 'air_pressure' in fp:
                    assert np.isclose(nc.variables['height'][:], -10.09)
                elif 'wind_speed' in fp:
                    assert np.isclose(nc.variables['height'][:], -11.06)
                elif 'wind_from_direction' in fp:
                    assert np.isclose(nc.variables['height'][:], -11.06)
                else:
                    assert nc.variables['height'][:].mask == True

    def test_waves(self):
        output = os.path.join(os.path.dirname(__file__), 'output')
        makedirs(output)
        main(output, 'sisp', 'waves')

        for f in os.listdir(output):
            fp = os.path.join(output, f)
            if '_waves_' not in fp:
                continue

            with nc4.Dataset(fp) as nc:

                assert 'latitude' in nc.variables
                assert np.isclose(nc.variables['latitude'][:], 27.862)

                assert 'longitude' in nc.variables
                assert np.isclose(nc.variables['longitude'][:], -80.445)

                assert nc.title == 'SISP'
                assert nc.description == 'Sebastian Inlet State Park, FL - ADCP and weather station'

                assert 'height' in nc.variables
                assert nc.variables['height'][:].mask == True
