import omni.kit.commands
from pxr import Gf, Sdf
import carb
import logging

# Sample Implementation of a DigitalTwin SDK property behaviour
# Receives Telemetry updates and converts it into a property change with custom business logic
# Office Building Sample, Convert Temperature to a Light Color (R, G, B) value
class RoomTemperatureBehavior(object):

    def __init__(self, live_layer, dtId, prim_path, default_temp):
        self.dtId = dtId
        self.usdPrimPath = prim_path
        self.live_layer = live_layer
        self.temperature = default_temp
        self.isDirty = True

    def _get_light_color_for_temperature(self, temp):

        # convert temperature to an RGB color
        # smoothly interpolating between the colors

        # <= 60.0 -> blue
        # == 67.5 -> cyan
        # == 75.0 -> green
        # == 82.5 -> yellow
        # >= 90.0 -> red

        # init rgb values

        color_r = 0
        color_g = 0
        color_b = 0

        if temp < 60.0:
            # anything below 60.0 is blue
            color_r = 0.0
            color_g = 0.0
            color_b = 1.0
        elif temp < 67.5:
            # from 60.0 to 67.5, interpolate from blue to cyan
            color_r = 0.0
            color_g = 1.0 - (67.5-temp) / 7.5
            color_b = 1.0
        elif temp < 75.0:
            # from 67.5 to 75.0, interpolate from cyan to green
            color_r = 0.0
            color_g = 1.0
            color_b = (75.0-temp) / 7.5
        elif temp < 82.5:
            # 75.0 to 82.5, interpolate from green to yellow
            color_r = 1.0 - (82.5-temp) / 7.5
            color_g = 1.0
            color_b = 0.0
        elif temp < 90:
            # 82.5 to 90, interpolate from yellow to red
            color_r = 1.0
            color_g = (90.0-temp) / 7.5
            color_b = 0.0
        else:
            # anything above 90.0 is red
            color_r = 1.0
            color_g = 0.0
            color_b = 0.0

        return color_r, color_g,  color_b

    def _setProperty(self, temperature):
        if (self._temperature != temperature):
            self._temperature = temperature
            self._isDirty = True

    def telemetryUpdate(self, key, value):
        if (key == '/Temperature'):
            self._setProperty(value)

    def handleTemperatureChange(self, stage):

        if (self._isDirty):
            r, g, b = self._get_light_color_for_temperature(self._temperature)
            prim_path = Sdf.Path(self._usdPrimPath)

            #carb.log_info("[digital.twin.starter.ext] Update Prim Path: {}".format(prim_path))
            #self._logger.info("changing property..." + self._templightpath + " " + str(r) + " " + str(g) + " " + str(b))

            omni.kit.commands.execute(
                "ChangeProperty",
                prop_path=Sdf.Path(prim_path.AppendProperty("inputs:color")),
                value=Gf.Vec3d(r, g, b),
                prev=Gf.Vec3d(r, g, b)
            )
            self._isDirty = False
