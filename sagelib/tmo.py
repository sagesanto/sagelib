import os
# to run the main function, need to uncomment this line:
from sagelib.observing_utils import get_angle, get_centroid, get_current_sidereal_time, dateToSidereal, find_transit_time, get_sunrise_sunset, get_hour_angle, angleToTimedelta, ensureFloat, ensureAngle, wrap_around, sidereal_rate, current_dt_utc
# and comment out this one:
# from sagelib.observing_utils import get_angle, get_centroid, get_current_sidereal_time, dateToSidereal, find_transit_time, get_sunrise_sunset, get_hour_angle, angleToTimedelta, ensureFloat, ensureAngle, wrap_around, sidereal_rate, current_dt_utc
import pytz, time
from datetime import datetime, timedelta, timezone
from astral import sun, LocationInfo
from astropy.coordinates import Angle
from astropy.table import Table, QTable
import astropy.units as u
import matplotlib.pyplot as plt, numpy as np

HORIZON_BOX = {  # {tuple(decWindow):tuple(minAlt,maxAlt)}
    (-35, -34): (-35, 42.6104),
    (-34, -32): (-35, 45.9539),
    (-32, -30): (-35, 48.9586),
    (-30, -28): (-35, 51.6945),
    (-28, -26): (-35, 54.2121),
    (-26, -24): (-35, 56.5487),
    (-24, -22): (-35, 58.7332),
    (-22, 0): (-35, 60),
    (0, 46): (-52.5, 60),
    (46, 56): (-37.5, 60),
    (56, 65): (-30, 60)
}

# flip the sign of the altitudes (second tuple) and their order in HORIZON_BOX to represent flipping the telescope
FLIP_BOX = {k:(-v[1],-v[0]) for k,v in HORIZON_BOX.items()}
print(FLIP_BOX)
# does this work??
def vertices_to_x_and_y(vertices):
    # take a list of tuples and return two lists, one of the x coords and one of the y coords
    return zip(*vertices)

class TMO:
    def __init__(self, flip_box=False):
        if flip_box:
            self.horizon_box = FLIP_BOX
        else:
            self.horizon_box = HORIZON_BOX
        self.flipped_box = flip_box
        self.locationInfo = LocationInfo(name="TMO", region="CA, USA",
                                timezone="UTC",
                                latitude=34.36,
                                longitude=-117.63)
        self._dec_vertices = list(set([item for key in self.horizon_box.keys() for item in
                         key]))  # this is just a list of integers, each being one member of one of the dec tuples that are the keys to the horizonBox dictionary
        self._dec_vertices.sort()
        self.horizon_box_vertices = self.get_horizon_box_vertices()
    
    def get_hour_angle_limits(self,dec):
        """
        Get the hour angle limits of the target's observability window based on its dec.
        @param dec: float, int, or astropy Angle
        @return: A tuple of Angle objects representing the upper and lower hour angle limits
        """
        dec = ensureFloat(dec)
        for decRange in self.horizon_box:
            if decRange[0] < dec <= decRange[1]:  # man this is miserable
                finalDecRange = self.horizon_box[decRange]
                return tuple([Angle(finalDecRange[0], unit=u.deg), Angle(finalDecRange[1], unit=u.deg)])
        return None        
    
    def static_observability_window(self, RA: Angle, Dec: Angle, target_dt=None,
                              current_sidereal_time=None):
        """!
        Generate the TMO observability window for a static target based on RA, dec, and location
        @param RA: right ascension
        @param Dec: declination
        @param locationInfo: astral LocationInfo object for the observatory site
        @param target_dt: find the next transit after this time. if None, uses currentTime
        @param current_sidereal_time: optional: the current sidereal time. calculating this ahead with observing_utils.get_current_sidereal_time and providing it to each function call vastly improves performance. will add sidereal days to this if necessary
        @return: [startTime, endTime]
        @rtype: list(datetime)
        """
        current_sidereal_time = current_sidereal_time if current_sidereal_time is not None else get_current_sidereal_time(self.locationInfo)

        target_dt = target_dt or current_dt_utc()
        t = find_transit_time(ensureAngle(RA), self.locationInfo, current_sidereal_time=current_sidereal_time,
                            target_dt=target_dt)
        time_window = (angleToTimedelta(a) for a in self.get_hour_angle_limits(Dec))
        return [t + a for a in time_window]
        # HA = ST - RA -> ST = HA + RA

    def get_sunrise_sunset(self, dt=current_dt_utc(), jd=False):
        """!
        get sunrise and sunset for TMO
        @return: sunriseUTC, sunsetUTC
        @rtype: datetime.datetime
        """
        return get_sunrise_sunset(self.locationInfo, dt=dt, jd=jd)
    
    def get_RA_window(self, target_dt, dec, ra=None, current_sidereal_time=None):
        # get the bounding RA coordinates of the TMO observability window for time target_dt for targets at declination dec. Optionally, input an RA to also get out that RA, adjusted for box-shifting

        current_sidereal_time = current_sidereal_time if current_sidereal_time is not None else get_current_sidereal_time(self.locationInfo)
        adjusted_ra = ra.copy() if ra is not None else None
        hourAngleWindow = self.get_hour_angle_limits(dec)
        if not hourAngleWindow: return False
        raWindow = [dateToSidereal(target_dt, current_sidereal_time) - hourAngleWindow[1],
                    (dateToSidereal(target_dt, current_sidereal_time) - hourAngleWindow[0]) % Angle(360, unit=u.deg)]

        # we want something like (23h to 17h) to look like [(23h to 24h) or (0h to 17h)] so we move the whole window to start at 0 instead
        if raWindow[0] > raWindow[1]:
            diff = Angle(24, unit=u.hour) - raWindow[0]
            raWindow[1] += diff
            if adjusted_ra is not None:
                adjusted_ra = (adjusted_ra + diff) % Angle(360, unit=u.deg)
            raWindow[0] = Angle(0, unit=u.deg)
        return raWindow, adjusted_ra

    def get_horizon_box_vertices(self):
        horizon_box_vertices = []
        for dec in self._dec_vertices:
            for offset in (0.5,-0.5):
                window = self.get_hour_angle_limits(dec+offset)
                if not window:
                    continue
                window = [a.deg for a in window]
                horizon_box_vertices.append((window[0],dec))
                horizon_box_vertices.append((window[1],dec))
        # put the vertices in clockwise order

        # find the centroid of the points
        centroid = get_centroid(horizon_box_vertices)
        # sort the points based on their angles with respect to the centroid
        ordered_vertices = sorted(horizon_box_vertices, key=lambda point: get_angle(point, centroid, centroid))
        # append the first vertex at the end to close the polygon
        ordered_vertices.append(ordered_vertices[0])
        # fix annoying shape malformation
        if self.flipped_box:
            ordered_vertices[30], ordered_vertices[31] = ordered_vertices[31], ordered_vertices[30]
        else:
            ordered_vertices[42], ordered_vertices[43] = ordered_vertices[43], ordered_vertices[42]

        return ordered_vertices

    def observation_viable(self, dt: datetime, ra: Angle, dec: Angle, current_sidereal_time=None):
        """
        Can a target with RA ra and Dec dec be observed at time dt? Checks hour angle limits based on TMO bounding box.
        @return: bool
        """
        current_sidereal_time = current_sidereal_time if current_sidereal_time is not None else get_current_sidereal_time(self.locationInfo)
        HA_window = self.get_hour_angle_limits(dec)
        HA = get_hour_angle(ra, dt, current_sidereal_time)
        night_time = self.is_at_night(dt)
        # NOTE THE ORDER:
        # if self.flipped_box:
        #     return HA.is_within_bounds(HA_window[1], HA_window[0]) and night_time
        return HA.is_within_bounds(HA_window[0], HA_window[1]) and night_time
    
    def is_at_night(self,dt:datetime):
        """ Is it night at TMO at time dt?"""
        sunrise, sunset = self.get_sunrise_sunset(dt)
        return sunset < dt < sunrise

    def observability_mask(self,table:QTable,current_sidereal_time=None,ra_column="ra",dec_column="dec",dt_column="dt"):
        """ Take a table of candidates and return a mask of which ones are observable at the current time"""
        current_sidereal_time = current_sidereal_time if current_sidereal_time is not None else get_current_sidereal_time(self.locationInfo)
        mask = np.zeros(len(table),dtype=bool)
        for i,row in enumerate(table):
            mask[i] = self.observation_viable(row[dt_column],row[ra_column],row[dec_column],current_sidereal_time=current_sidereal_time)
        return mask

    def plot_onsky(self, dt=current_dt_utc(),candidates=None,current_sidereal_time=None, fig=None, ax=None):
        """ Take a list of candidates, create 2 plots of their observability at dt, and return the figures, axes and artists (to allow animation)"""
        sunrise, sunset = self.get_sunrise_sunset(dt)
        current_sidereal_time = current_sidereal_time if current_sidereal_time is not None else get_current_sidereal_time(self.locationInfo)
        sidereal = dateToSidereal(dt, current_sidereal_time)
        names = [c.CandidateName for c in candidates]
        ras = [c.RA for c in candidates]
        # print("RA type:",type(ras[0]))
        decs = [c.Dec for c in candidates]
        table = QTable([names,ras,decs],names=["name","RA","Dec"])
        # calculate hour angles
        # print("Table RA:",table["RA"])
        # print("Table RA Type:",table["RA"].dtype)
        # print("Current sidereal:",sidereal)
        table["HA"] = [get_hour_angle(ra,dt,current_sidereal_time).deg for ra in table["RA"]]
        # print("Table HA:",table["HA"])
        # make column indicating which targets are observable
        # this line used to look for obs viable at dt-timedelta(day=1):
        table["Observable"] = [self.observation_viable(dt,Angle(row["RA"],unit='deg'),Angle(row["Dec"],unit='deg'), current_sidereal_time=current_sidereal_time) for row in table]

        x,y = vertices_to_x_and_y(self.horizon_box_vertices)
        # graph min needs to become more negative if already negative else less positive
        min_coeff = 1.1 if min(x) < 0 else 0.9

        xlims = [(-180,180),(min_coeff*min(x),1.1*max(x))]
        min_coeff = 1.1 if min(y) < 0 else 0.9
        ylims = [(-90,90),(min_coeff*min(y),1.1*max(y))]
        figsizes = [(20,5),(10,10)]
        tables = [table,table[table["Observable"]]]
        artists_ls = []
        fig_ax_pairs = []
        for data, figsize, xlimits, ylimits in zip(tables, figsizes,xlims,ylims):
            fig, ax = plt.subplots(figsize=figsize)
            # Plot the polygon outline
            colors = plt.cm.tab20(np.linspace(0, 1, len(data)))
            # draw the ha box
            ax.cla()
            # p = ax.plot(x, y, color='green', linestyle='dashed')
            # HA = [wrap_around(ha) for ha in table["HA"]]
            HA = table["HA"]
            # print("HA:",HA)
            ax.set_xlabel('HA (deg)')
            ax.set_ylabel('Dec (deg)')
            plt.axis('scaled')
            ax.set_xlim(*xlimits)
            ax.set_ylim(*ylimits)
            plt.title(f"Observability at {dt.strftime('%Y-%m-%d %H:%M:%S')} UTC")
            sunr, suns = dateToSidereal(sunrise,current_sidereal_time), dateToSidereal(sunset, current_sidereal_time)
            sunr, suns = sunr-sidereal, suns-sidereal
            sunr, suns = wrap_around(sunr.deg), wrap_around(suns.deg)
            sunrise_line = ax.axvline(x=sunr, linestyle='--', color='red')
            sunset_line = ax.axvline(x=suns, linestyle='--', color='blue')

            if suns < sunr:
                fill = ax.axvspan(suns, sunr, alpha=0.2, color='gray')
            else:
                fill1 = ax.axvspan(xlimits[0],sunr,alpha=0.2,color="gray")
                fill2 = ax.axvspan(suns,xlimits[1],alpha=0.2,color="gray")

            artists = [ax.plot(x, y, color='green', linestyle='dashed'),sunrise_line,sunset_line]
            
            # to label the vertices of the box for debugging:
            # for i, p in enumerate(zip(x,y)):
            #     px,py = p
            #     artists.append(ax.text(px,py,s=str(i)))

            for i, row in enumerate(data):
                artists.append(ax.scatter(row["HA"], row["Dec"], c=[colors[i]], label=row["name"],s=10))
                # print("HA:",row["HA"])
            artists_ls.append(artists)
            fig_ax_pairs.append((fig,ax))
        return fig_ax_pairs, artists_ls
    
if __name__ == "__main__":
    class Candidate:
        def __init__(self,RA:Angle, Dec, CandidateName):
            self.RA = RA 
            self.Dec = Dec
            self.CandidateName = CandidateName

    tmo = TMO()
    lst = get_current_sidereal_time(tmo.locationInfo)
    t = find_transit_time(lst,tmo.locationInfo)
    print("Current time:",current_dt_utc())
    print("Hour angle:",get_hour_angle(lst,t,lst))
    print("Transit time:", find_transit_time(RA=lst,location=tmo.locationInfo,target_dt=t))
    c = [Candidate(**{"RA":lst,"Dec":0,'CandidateName':"test"})]
    t = find_transit_time(c[0].RA,tmo.locationInfo) + timedelta(hours=1.5)
    tmo.plot_onsky(candidates=c,dt=t)
    print("Observable:",tmo.observation_viable(t,lst,0))
    plt.show()
    # plt.show(block=False)