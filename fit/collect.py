#!python
# coding=utf-8

import os
import csv
import shutil
import logging
import tempfile
import requests
import argparse
from copy import copy
from glob import glob
from datetime import datetime

import pytz
import netCDF4
import numpy as np
import pandas as pd
from dateutil.parser import parse as dateparse

from pyaxiom.netcdf.sensors import TimeSeries
from pyaxiom.netcdf.dataset import EnhancedDataset
from pyaxiom.utils import urnify
from pyaxiom.urn import IoosUrn

# Log to stdout
logger = logging.getLogger()
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.DEBUG)

# Don't show the HTTP connection spam
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("pyaxiom").setLevel(logging.ERROR)

# http://my.fit.edu/coastal/downloadable_data/Headers/FIT_SISPNJ%20Header.txt
met_header   = ['year', 'month', 'day', 'hour', 'minute', 'second', 'batt_vol', 'atmp', 'baro', 'wspd', 'wdir', 'wtmp', 'salt', 'wlvl']
met_mapping    = {
    'atmp': {
        'standard_name': 'air_temperature',
        'units': 'degree_Celsius',
        'keywords': 'Atmosphere > Atmosphere Temperature -> Surface Temperature -> Air Temperature',
        'height_above_site': 0.3
    },
    'baro': {
        'standard_name': 'air_pressure',
        'units': 'mbar',
        'keywords': 'Oceans > Ocean Pressure > Sea Level Pressure',
        'height_above_site': 0.3
    },
    'wspd': {
        'standard_name': 'wind_speed',
        'units': 'm.s-1',
        'keywords': 'Oceans > Ocean Winds > Surface Winds',
        'height_above_site': 1.
    },
    'wdir': {
        'standard_name': 'wind_from_direction',
        'units': 'degrees',
        'keywords': 'Oceans > Ocean Winds > Surface Winds',
        'height_above_site': 1.
    },
    'wtmp': {
        'standard_name': 'sea_water_temperature',
        'units': 'degree_Celsius',
        'keywords': 'Oceans > Ocean Temperature > Water Temperature',
    },
    'salt': {
        'standard_name': 'sea_water_practical_salinity',
        'units': 'PSS',
        'keywords': 'Oceans > Salinity/Density > Salinity'
    },
    'wlvl': {
        'standard_name': 'water_surface_height_above_reference_datum',
        'units': 'm',
        'keywords': 'Oceans > Coastal Processes > Sea Surface Height',
        'vertical_datum': 'MLLW'
    }
}

currents_header = ['year', 'month', 'day', 'hour', 'minute', 'second', 'cspd', 'cdir', 'wtmp', 'wlvl', 'heading', 'pitch', 'roll', 'magnetic_dir']
currents_mapping = {
    'cspd': {
        'standard_name': 'sea_water_speed',
        'units': 'm.s-1',
        'keywords': 'Oceans > Ocean Circulation > Ocean Currents',
    },
    'cdir': {
        'standard_name': 'sea_water_direction',
        'units': 'degrees',
        'keywords': 'Oceans > Ocean Circulation > Ocean Currents',
    },
    'wtmp': {
        'standard_name': 'sea_water_temperature',
        'units': 'degree_Celsius',
        'discriminant': 'adcp',
        'keywords': 'Oceans > Ocean Temperature > Water Temperature',
        'depth_below_surface':  8.53
    },
    'wlvl': {
        'standard_name': 'sea_floor_depth_below_sea_surface',
        'units': 'm',
        'discriminant': 'adcp',
        'keywords': 'Oceans > Coastal Processes > Sea Surface Height',
        'add_offset': 2.4
    }
}

waves_header = ['year', 'month', 'day', 'hour', 'minute', 'second', 'sgwh', 'pkwp', 'pkwd', 'magnetic_dir']
waves_mapping = {
    'sgwh': {
        'standard_name': 'sea_surface_wave_significant_height',
        'units': 'm',
        'keywords': 'Oceans > Ocean Waves > Significant Wave Height',
    },
    'pkwp': {
        'standard_name': 'sea_surface_dominant_wave_period',
        'units': 's',
        'keywords': 'Oceans > Ocean Waves > Wave Period',
    },
    'pkwd': {
        'standard_name': 'sea_surface_wave_to_direction',
        'units': 'degrees',
        'keywords': 'Oceans > Ocean Waves > Wave Speed/Direction',
    }
}

stations = {
    'sisp' : {
        'latitude': 27.862,
        'longitude': -80.445,
        'site_height': 10.06,
        'title': 'SISP',
        'description': 'Sebastian Inlet State Park, FL - ADCP and weather station'
    }
}

global_attributes = {
    'naming_authority':         'edu.fit',
    'source':                   'FIT',
    'institution':              'Florida Institute of Technology',
    'project':                  'Florida Institute of Technology Coastal Program',
    'creator_email':            'zarillo@fit.edu',
    'creator_name':             'Florida Institute of Technology',
    'creator_institution':      'Florida Institute of Technology',
    'creator_url':              'http://fit.edu',
    'creator_type':             'institution',
    'publisher_email':          'vembu@secoora.org',
    'publisher_name':           'SECOORA',
    'publisher_institution':    'SECOORA',
    'publisher_type':           'institution',
    'publisher_url':            'http://secoora.org',
    'contributor_name':         'Gary Zarillo, Irene Watts',
    'contributor_role':         'principalInvestigator, technician',
    'Conventions':              'CF-1.6',
    'standard_name_vocabulary': 'CF-1.6',
    'keywords_vocabulary':      'GCMD Science Keywords',
    'date_created':             datetime.utcnow().strftime("%Y-%m-%dT%H:%M:00Z"),
    'license':                  'The data available here are intended solely for educational use by the academic and scientific community, with the express understanding that any such use will properly acknowledge the originating investigator. Anyone wishing to use these data in a presentation, report, thesis or publication should contact the originating investigator. It is expected that all customary courtesies and privileges attached to data use will be strictly honored. Use or reproduction of any material herein for any commercial purpose is prohibited without prior written permission from the Department of Marine and Environmental Systems at Florida Institute of Technology.'
}


def main(output, station, datatype):

    if datatype == 'met':
        headers = met_header
        mapping = met_mapping
    elif datatype == 'waves':
        headers = waves_header
        mapping = waves_mapping
    elif datatype == 'currents':
        headers = currents_header
        mapping = currents_mapping

    df = None

    def dp(*args):
        datestr = "".join([ str(x) for x in args ])
        try:
            return datetime.strptime(datestr, '%Y %m %d %H %M %S')
        except ValueError:
            return np.nan

    datapath = os.path.abspath(os.path.join(os.path.dirname(__file__), 'data', datatype))
    for csv_file in sorted(os.listdir(datapath)):
        f = os.path.join(datapath, csv_file)
        cf = pd.read_csv(
            f,
            header=None,
            names=headers,
            parse_dates={'time': ['year', 'month', 'day', 'hour', 'minute', 'second']},
            date_parser=dp
        )
        cf.dropna(subset=['time'], inplace=True)

        if df is None:
            df = cf
        else:
            df = df.append(cf)

    fillvalue = -9999.9
    # Station metadata
    stat_meta = stations[station]
    station_urn = IoosUrn(asset_type='station',
                          authority=global_attributes['naming_authority'],
                          label=stat_meta['title'])

    for var in df.columns:

        try:
            var_meta = mapping[var]
        except KeyError:
            logger.error("Variable {!s} was not found in variable map!".format(var))
            continue

        sensor_urn = urnify(station_urn.authority, station_urn.label, var_meta)

        gas = copy(global_attributes)
        gas['keywords'] = var_meta['keywords']
        gas['title'] = stat_meta['title']
        gas['description'] = stat_meta['description']

        skip_variable_attributes = ['keywords', 'height_above_site', 'depth_below_surface', 'add_offset']
        vas = { k: v for k, v in var_meta.items() if k not in skip_variable_attributes }

        if var_meta.get('height_above_site') and stat_meta.get('site_height'):
            # Convert to positive down
            df['depth'] = -1 * (stat_meta['site_height'] + var_meta['height_above_site'])
        else:
            df['depth'] = var_meta.get('depth_below_surface', np.nan)

        if 'add_offset' in var_meta:
            df[var] = df[var] + var_meta['add_offset']

        output_filename = '{}_{}_{}.nc'.format(station_urn.label, datatype, var_meta['standard_name'])
        ts = TimeSeries.from_dataframe(
            df, 
            output,
            output_filename,
            stat_meta['latitude'],
            stat_meta['longitude'],
            station_urn.urn,
            gas,
            var_meta["standard_name"],
            vas,
            sensor_vertical_datum=var_meta.get('vertical_datum'),
            fillvalue=fillvalue,
            data_column=var,
            vertical_axis_name='height',
            vertical_positive='down'
        )
        ts.add_instrument_metadata(urn=sensor_urn)


if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument('-o', '--output',
                        required=True,
                        help="Directory to output NetCDF files to",
                        nargs='?')
    parser.add_argument('-s', '--station',
                        required=True,
                        help="Station to process",
                        choices=['sisp'],
                        nargs='+')
    parser.add_argument('-d', '--datatype',
                        required=True,
                        help="Specific type to process",
                        choices=['currents', 'waves', 'met'],
                        nargs='+')
    args = parser.parse_args()

    main(args.output, args.station, args.datatype)
