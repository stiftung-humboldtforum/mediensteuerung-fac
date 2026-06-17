import asyncio
import os
import time
from threading import Thread

from pysnmp.entity import engine, config
from pysnmp.carrier.asyncio.dgram import udp
from pysnmp.entity.rfc3413 import ntfrcv

import paho.mqtt.client as mqtt

PORT = 8080
COMMUNITYSTRING = os.environ['COMMUNITYSTRING']


class App(Thread):
    def __init__(self, mqtt_client, *args, **kwargs):
        super().__init__(*args, daemon=True, **kwargs)
        self.mqtt_client = mqtt_client

    def run(self):
        # pysnmp's asyncio carrier grabs asyncio.get_event_loop() at engine
        # construction; on a non-main thread that raises unless a loop is set.
        asyncio.set_event_loop(asyncio.new_event_loop())

        snmpEngine = engine.SnmpEngine()
        config.add_v1_system(snmpEngine, COMMUNITYSTRING, COMMUNITYSTRING)
        ntfrcv.NotificationReceiver(snmpEngine, self.cbFun)
        self.add_transport(snmpEngine, PORT)
        snmpEngine.transport_dispatcher.job_started(1)
        try:
            print("Trap Listener started .....")
            print("To Stop Press Ctrl+c")
            print("\n")
            snmpEngine.open_dispatcher()
        except:
            snmpEngine.close_dispatcher()
            raise

    def add_transport(self, snmpEngine, PORT):
        """
        :param snmpEngine:
        :return:
        """
        try:
            config.add_transport(
                snmpEngine,
                udp.DOMAIN_NAME,
                udp.UdpTransport().open_server_mode(('0.0.0.0',
                                                     int(PORT)))
            )
        except Exception as e:
            print("{} Port Binding Failed the Provided Port {} is in Use".format(e, PORT))

    def cbFun(self, snmpEngine, stateReference, contextEngineId, context, *_):
        print(stateReference)
        # Diagnostic only: must never prevent the trap from being published.
        try:
            execContext = snmpEngine.observer.get_execution_context(
                'rfc3412.receiveMessage:request'
            )
            print('#Notification from %s \n#ContextEngineId: "%s" \n#SecurityName "%s"' %
                  (execContext['transportAddress'],
                   contextEngineId,
                   execContext['securityName'])
                  )
        except Exception as e:
            print(f'#execution context unavailable: {e}')
        # NOTE: `context` is the callback's 4th positional arg (contextName), NOT
        # the trap varBinds. This is pre-existing behaviour kept verbatim to
        # preserve the fac/{payload} wire format that the manager parses. Do NOT
        # switch this to varBinds without verifying against a real BMZ trap —
        # changing what is published here breaks the fire-alarm scram path.
        payload = context[-1][-1]
        print(f'#Payload: {payload}')
        self.mqtt_client.publish(f'fac/{payload}')


if __name__ == "__main__":
    while True:
        mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id='fac')
        mqtt_client.on_connect = print  # set before connect so CONNACK isn't missed
        mqtt_client.tls_set(
            '/opt/tls/ca_certificate.pem',
            '/opt/tls/client_certificate.pem',
            '/opt/tls/client_key.pem'
        )
        mqtt_client.connect(os.environ['MQTT_HOSTNAME'], 8883)
        mqtt_client.loop_start()
        app = App(mqtt_client)
        try:
            app.start()
            app.join()
        finally:
            # Tear down the network thread + connection before re-looping so a
            # dead SNMP dispatcher doesn't leak an MQTT client each iteration.
            mqtt_client.loop_stop()
            try:
                mqtt_client.disconnect()
            except Exception:
                pass
        time.sleep(5)
