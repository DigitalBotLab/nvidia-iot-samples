import json
import threading
import omni.kit.pipapi
import azure.eventhub

try:
    omni.kit.pipapi.install("azure-eventhub", module="azure-eventhub", ignore_import_check=True, ignore_cache=True, surpress_output=False,use_online_index=True )
    from azure.eventhub import EventHubConsumerClient
    from azure.eventhub import EventData
except:
    omni.kit.pipapi.install("azure-eventhub", module="azure-eventhub", ignore_import_check=True, ignore_cache=True, surpress_output=False,use_online_index=True )
    from azure.eventhub import EventHubConsumerClient
    from azure.eventhub import EventData

# see https://learn.microsoft.com/en-us/samples/azure/azure-sdk-for-python/eventhub-samples

# handles incoming digital twin property changes being published from an azure event hub

class DigitalTwinConnectClient:

    def __init__(self, connection_string, behaviors, logger):
        if (connection_string != ""):
            self._ehcc = EventHubConsumerClient.from_connection_string(conn_str=connection_string, consumer_group="$Default")
            self._behaviors = behaviors
            self._logger = logger


    def _process_event(self, context, event):

        self._logger.info("received event...")
        self._logger.info(" > context: {}".format(context))
        self._logger.info(" > event: {}".format(event))

        event_props_raw = event.properties
        # convert properties dictionary:
        event_props = {k.decode('UTF-8'): event_props_raw.get(k).decode('UTF-8') for k in event_props_raw.keys()}

        # parse the source to get the twin Id
        subject = event_props.get('cloudEvents:subject')
        result_list = subject.split('/')
        subject = result_list[-1]
        self._logger.info(" > subject: {}".format(subject))

        event_body = event.body_as_json()

        self.process_telemetry(subject, event_body['patch'][0]['path'], event_body['patch'][0]['value'])

    def process_telemetry(self, dtid, path, value):
        self._logger.info("received telemetry...")
        self._logger.info(" > DtID: {}".format(dtid))
        self._logger.info(" > handling telemetry item; path={} value={}".format(path, value))

        for behavior in self._behaviors:
            behavior.telemetryUpdate(self, path, value)


    def start_listening(self):
        self._worker = threading.Thread(
            target=self._ehcc.receive,
            kwargs={
                "on_event": self._process_event,
                "owner_level": 1
            }
        )
        self._worker.start()
        self._logger.info("listening...")

    def stop_listening(self):
        self._logger.info("stopping...")
        self._ehcc.close()
        self._worker = None
