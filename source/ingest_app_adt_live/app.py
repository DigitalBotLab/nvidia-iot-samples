# SPDX-FileCopyrightText: Copyright (c) 2023 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: MIT
#
# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the "Software"),
# to deal in the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense,
# and/or sell copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
# THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
# DEALINGS IN THE SOFTWARE.

# pip install pandas

import asyncio
import os
import omni.client
import json
from pxr import Usd, Sdf
from pathlib import Path
import pandas as pd
import time
from ..digital_twin_sdk.business_logic import RoomTemperatureBehavior
from ..digital_twin_sdk.eventhub_client import DigitalTwinConnectClient



OMNI_HOST = os.environ.get("OMNI_HOST", "localhost")
BASE_URL = "omniverse://" + OMNI_HOST + "/Projects/ADT/Samples/HeadlessApp"
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
CONTENT_DIR = Path(SCRIPT_DIR).resolve().parents[1].joinpath("content")
ADT_HOST =  os.environ.get("ADT_EVENTHUB")

messages = []

def log_handler(thread, component, level, message):
    # print(message)
    messages.append((thread, component, level, message))


def createBehavior(live_layer, root_path, path, behaviorType, dtId):
    if behaviorType == "RoomTemperatureBehavior":
        behavior = RoomTemperatureBehavior(live_layer, dtId, path, 75.0)

        iot_root = live_layer.GetPrimAtPath(root_path)
        if not iot_root:
            iot_root = Sdf.PrimSpec(live_layer, root_path, Sdf.SpecifierDef, "OBS Root")

        iot_spec = live_layer.GetPrimAtPath(path)
        if not iot_spec:
            iot_spec = Sdf.PrimSpec(iot_root, path, Sdf.SpecifierDef, "RoomTemperatureBehavior Type")
        if not iot_spec:
            raise Exception("Failed to create the OBS Spec.")

        # clear out any attrubutes that may be on the spec
        for attrib in iot_spec.attributes:
            iot_spec.RemoveProperty(attrib)

        # create all the attributes that will be written
        attr = Sdf.AttributeSpec(iot_spec, "inputs:color", Sdf.ValueTypeNames.Vector3d)
        if not attr:
            raise Exception(f"Could not define the attribute: inputs:color")

        return behavior



def create_live_layer(iot_topic):
    LIVE_URL = f"{BASE_URL}/{iot_topic}.live"

    live_layer = Sdf.Layer.CreateNew(LIVE_URL)
    if not live_layer:
        raise Exception(f"Could load the live layer {LIVE_URL}.")

    Sdf.PrimSpec(live_layer, "iot", Sdf.SpecifierDef, "IoT Root")
    live_layer.Save()
    return live_layer


def initialize_async(adt_model_map):
#async def initialize_async(adt_model_map):

    behaviors = []

    #load the Azure Digital Twin behavior/model mapping file
    with open(adt_model_map) as json_file:
        adt_model = json.load(json_file)

    usd_topic = adt_model.usdFilePath.replace('.usd', '')

    # copy a the USD File to the target nucleus server
    LOCAL_URL = f"file:{CONTENT_DIR}/{adt_model.usdFilePath}"
    STAGE_URL = f"{BASE_URL}/{adt_model.usdFilePath}"
    LIVE_URL = f"{BASE_URL}/{usd_topic}.live"

    result = omni.client.copy_async(
        LOCAL_URL,
        STAGE_URL,
        behavior=omni.client.CopyBehavior.ERROR_IF_EXISTS,
        message="Copy Sample USD",
    )

#    result = await omni.client.copy_async(
#         LOCAL_URL,
#         STAGE_URL,
#         behavior=omni.client.CopyBehavior.ERROR_IF_EXISTS,
#         message="Copy Sample USD",
#     )

    stage = Usd.Stage.Open(STAGE_URL)
    if not stage:
        raise Exception(f"Could load the stage {STAGE_URL}.")

    root_layer = stage.GetRootLayer()
    live_layer = Sdf.Layer.FindOrOpen(LIVE_URL)
    if not live_layer:
        live_layer = create_live_layer(iot_topic)

    found = False
    subLayerPaths = root_layer.subLayerPaths
    for subLayerPath in subLayerPaths:
        if subLayerPath == live_layer.identifier:
            found = True

    if not found:
        root_layer.subLayerPaths.append(live_layer.identifier)
        root_layer.Save()

    # set the live layer as the edit target
    stage.SetEditTarget(live_layer)

    #initialize a behavior for each behavior in the map
    for behaviorMap in adt_model.behaviors:
        for path in behaviorMap.paths: #Create a new behavior for each USD path
            behaviors.append(createBehavior(live_layer, behaviorMap.behaviorType, behaviorMap.dtid, path))

    omni.client.live_process()
    return stage, live_layer, behaviors

# def write_to_live(live_layer, iot_topic, group, ts):
#     # write the iot values to the usd prim attributes
#     print(group.iloc[0]["TimeStamp"])
#     ts_attribute = live_layer.GetAttributeAtPath(f"/iot/{iot_topic}._ts")
#     ts_attribute.default = ts
#     with Sdf.ChangeBlock():
#         for index, row in group.iterrows():
#             id = row["Id"]
#             value = row["Value"]
#             attr = live_layer.GetAttributeAtPath(f"/iot/{iot_topic}.{id}")
#             if not attr:
#                 raise Exception(f"Could not find attribute /iot/{iot_topic}.{id}.")
#             attr.default = value
#     omni.client.live_process()

def run(stage, live_layer, behaviors):

    #Connect the Event Hub - Listen for events
    _listening = False
    _dtcc = DigitalTwinConnectClient(ADT_HOST, behaviors, log_handler)

    # play back the data in real-time
    #for next_time, group in grouped:
    #    diff = (next_time - last_time).total_seconds()
    #    if diff > 0:
    #        time.sleep(diff)
    #    write_to_live(live_layer, iot_topic, group, (next_time - start_time).total_seconds())
    #    last_time = next_time

if __name__ == "__main__":
    ADT_MODEL_MAP = f"{CONTENT_DIR}/adt-office-building-map.json"

    omni.client.initialize()
    omni.client.set_log_level(omni.client.LogLevel.DEBUG)
    omni.client.set_log_callback(log_handler)

    try:
        #stage, live_layer, behaviors = asyncio.run(initialize_async(ADT_MODEL_MAP))
        stage, live_layer, behaviors = initialize_async(ADT_MODEL_MAP)
        #run(stage, live_layer, behaviors)
    finally:
        omni.client.shutdown()
