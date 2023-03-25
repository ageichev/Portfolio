import pandas as pd
import numpy as np
import sys
import inspect


def haversine(p_long, p_lat, d_long, d_lat, radius=6371.):
    p_long, p_lat, d_long, d_lat = map(np.radians, [p_long, p_lat, d_long, d_lat])
    diff_lon = d_long - p_long
    diff_lat = d_lat - p_lat
    
    a = np.sin(diff_lat/2.0)**2 + np.cos(p_lat) * np.cos(d_lat) * np.sin(diff_lon/2.0)**2
    c = 2 * np.arcsin(np.sqrt(a))
    d = radius * c
    
    return d

nyc['distance'] = haversine(nyc.pickup_longitude.values, nyc.pickup_latitude.values, 
                                nyc.dropoff_longitude.values, nyc.dropoff_latitude.values)

