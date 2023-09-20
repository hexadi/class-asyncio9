import time
import random
import json
import asyncio
import aiomqtt
from enum import Enum

student_id = "6310301018"

# State 
S_OFF       = 'OFF'
S_READY     = 'READY'
S_FAULT     = 'FAULT'
S_FILLWATER = 'FILLWATER'
S_HEATWATER = 'HEATWATER'
S_WASH      = 'WASH'
S_RINSE     = 'RINSE'
S_SPIN      = 'SPIN'

# Function
S_DOORCLOSED            = 'DOORCLOSE'
S_FULLLEVELDETECTED     = 'FULLLEVELDETECTED'
S_TEMPERATUREREACHED    = 'TEMPERATUREREACHED'
S_FUNCTIONCOMPLETED     = 'FUNCTIONCOMPLETED'
S_TIMEOUT               = 'TIMEOUT'
S_MOTORFAILURE          = 'MOTORFAILURE'
S_OUTOFBALANCE          = 'OUTOFBALANCE'
S_FAULTCLEARED          = 'FAULTCLEARED'

async def publish_message(w, client, app, action, name, value):
    await asyncio.sleep(1)
    payload = {
                "action"    : "get",
                "project"   : student_id,
                "model"     : "model-01",
                "serial"    : w.SERIAL,
                "name"      : name,
                "value"     : value
            }
    print(f"{time.ctime()} - PUB topic: v1cdti/{app}/{action}/{student_id}/model-01/{w.SERIAL} payload: {name}:{value}")
    await client.publish(f"v1cdti/{app}/{action}/{student_id}/model-01/{w.SERIAL}"
                        , payload=json.dumps(payload))

async def fillwater(w, filltime=100):
    print(f"{time.ctime()} - [{w.SERIAL}] Waiting for fill water maxinum {filltime} seconds")
    await asyncio.sleep(filltime)

async def heatwater(w, filltime=100):
    print(f"{time.ctime()} - [{w.SERIAL}] Waiting for heat water maxinum {filltime} seconds")
    await asyncio.sleep(filltime)

async def wash(w, filltime=100):
    print(f"{time.ctime()} - [{w.SERIAL}] Waiting for wash maxinum {filltime} seconds")
    await asyncio.sleep(filltime)

async def rinse(w, filltime=100):
    print(f"{time.ctime()} - [{w.SERIAL}] Waiting for rinse maxinum {filltime} seconds")
    await asyncio.sleep(filltime)

async def spin(w, filltime=100):
    print(f"{time.ctime()} - [{w.SERIAL}] Waiting for spin maxinum {filltime} seconds")
    await asyncio.sleep(filltime)

class MachineStatus(Enum):
    pressure = round(random.uniform(2000,3000), 2)
    temperature = round(random.uniform(25.0,40.0), 2)

class MachineMaintStatus(Enum):
    filter = random.choice(["clear", "clogged"])
    noise = random.choice(["quiet", "noisy"])

class WashingMachine:
    def __init__(self, serial):
        self.SERIAL = serial
        self.STATE = 'OFF'
        self.Task = None
        self.event = asyncio.Event()

async def CoroWashingMachine(w, client):

    while True:
        if w.STATE == S_OFF:
            print(f"{time.ctime()} - [{w.SERIAL}-{w.STATE}]")
            await w.event.wait()
            w.event.clear()
            continue

        if w.STATE == S_FAULT:
            print(f"{time.ctime()} - [{w.SERIAL}-{w.STATE}]")
            # await asyncio.sleep(wait_next)
            await w.event.wait()
            w.event.clear()
            continue

        if w.STATE == S_READY:
            print(f"{time.ctime()} - [{w.SERIAL}-{w.STATE}]")

            await publish_message(w, client, "app", "get", "STATUS", S_READY)
            # door close
            
            await publish_message(w, client, "app", "get", "STATUS", S_FILLWATER)
            w.STATE = S_FILLWATER

        if w.STATE == S_FILLWATER:
            # fill water untill full level detected within 10 seconds if not full then timeout 
            w.Task = asyncio.create_task(fillwater(w),name=S_FILLWATER)
            try:
                await asyncio.wait_for(w.Task, timeout=10.0)
            except asyncio.exceptions.TimeoutError:
                w.Task = None
                await publish_message(w, client, "app", "set", "STATUS", S_FAULT)
                w.STATE = S_FAULT
                continue
            except asyncio.exceptions.CancelledError:
                if w.STATE == S_FULLLEVELDETECTED:
                    await publish_message(w, client, "app", "get", "STATUS", S_HEATWATER)
                    w.STATE = S_HEATWATER
        if w.STATE == S_HEATWATER:
            # heat water until temperature reach 30 celcius within 10 seconds if not reach 30 celcius then timeout
            w.Task = asyncio.create_task(heatwater(w),name=S_HEATWATER)
            try:
                await asyncio.wait_for(w.Task, timeout=10.0)
            except asyncio.exceptions.TimeoutError:
                w.Task = None
                await publish_message(w, client, "app", "set", "STATUS", S_FAULT)
                w.STATE = S_FAULT
                continue
            except asyncio.exceptions.CancelledError:
                if w.STATE == S_TEMPERATUREREACHED:
                    await publish_message(w, client, "app", "get", "STATUS", S_WASH)
                    w.STATE = S_WASH
        if w.STATE == S_WASH:
            # wash 10 seconds, if out of balance detected then fault
            w.Task = asyncio.create_task(wash(w),name=S_WASH)
            try:
                await asyncio.wait_for(w.Task, timeout=10.0)
            except asyncio.exceptions.TimeoutError:
                await publish_message(w, client, "app", "get", "STATUS", S_RINSE)
                w.STATE = S_RINSE
            except asyncio.exceptions.CancelledError:
                if w.STATE == S_OUTOFBALANCE:
                    w.Task = None
                    await publish_message(w, client, "app", "set", "STATUS", S_FAULT)
                    w.STATE = S_FAULT
                    continue
        if w.STATE == S_RINSE:
            # rinse 10 seconds, if motor failure detect then fault
            w.Task = asyncio.create_task(rinse(w),name=S_RINSE)
            try:
                await asyncio.wait_for(w.Task, timeout=10.0)
            except asyncio.exceptions.TimeoutError:
                await publish_message(w, client, "app", "get", "STATUS", S_SPIN)
                w.STATE = S_SPIN
            except asyncio.exceptions.CancelledError:
                if w.STATE == S_MOTORFAILURE:
                    w.Task = None
                    await publish_message(w, client, "app", "set", "STATUS", S_FAULT)
                    w.STATE = S_FAULT
                    continue
        if w.STATE == S_SPIN:
            # spin 10 seconds, if motor failure detect then fault
            w.Task = asyncio.create_task(spin(w),name=S_SPIN)
            try:
                await asyncio.wait_for(w.Task, timeout=10.0)
            except asyncio.exceptions.TimeoutError:
                await publish_message(w, client, "app", "get", "STATUS", S_OFF)
                w.STATE = S_OFF
            except asyncio.exceptions.CancelledError:
                if w.STATE == S_MOTORFAILURE:
                    w.Task = None
                    await publish_message(w, client, "app", "set", "STATUS", S_FAULT)
                    w.STATE = S_FAULT
                    continue

        # When washing is in FAULT state, wait until get FAULTCLEARED
            

async def listen(w, client):
    async with client.messages() as messages:
        print(f"{time.ctime()} - SUB topic: v1cdti/hw/set/{student_id}/model-01/{w.SERIAL}")
        await client.subscribe(f"v1cdti/hw/set/{student_id}/model-01/{w.SERIAL}")
        await client.subscribe(f"v1cdti/app/get/{student_id}/model-01/")
        async for message in messages:
            m_decode = json.loads(message.payload)
            if message.topic.matches(f"v1cdti/hw/set/{student_id}/model-01/{w.SERIAL}") and m_decode["serial"] == w.SERIAL and m_decode["project"] == student_id and m_decode["action"] == "set" and m_decode["model"] == "model-01":
                # set washing machine status
                print(f"{time.ctime()} - MQTT [{m_decode['serial']}]:{m_decode['name']} => {m_decode['value']}")
                if (m_decode['name']=="STATUS" and m_decode['value']==S_FAULTCLEARED):
                    w.STATE = S_READY
                    w.event.set()
                elif (m_decode['name']=="STATUS" and m_decode['value']==S_READY and w.STATE != S_FAULT):
                    w.STATE = S_READY
                    w.event.set()
                elif (m_decode['name']=="STATUS" and m_decode['value']==S_FAULT):
                    w.STATE = S_FAULT
                elif (m_decode['name']=="STATUS" and m_decode['value']==S_OFF):
                    w.STATE = "OFF"
                elif (w.Task is not None):
                    if (w.Task.get_name() == S_FILLWATER and m_decode['name']=="STATUS" and m_decode['value']==S_FULLLEVELDETECTED):
                        w.STATE = S_FULLLEVELDETECTED
                        w.Task.cancel()
                    elif (w.Task.get_name() == S_HEATWATER and m_decode['name']=="STATUS" and m_decode['value']==S_TEMPERATUREREACHED):
                        w.STATE = S_TEMPERATUREREACHED
                        w.Task.cancel()
                    elif (w.Task.get_name() == S_WASH and m_decode['name']=="STATUS" and m_decode['value']==S_OUTOFBALANCE):
                        w.STATE = S_OUTOFBALANCE
                        w.Task.cancel()
                    elif (w.Task.get_name() == S_RINSE and m_decode['name']=="STATUS" and m_decode['value']==S_MOTORFAILURE):
                        w.STATE = S_MOTORFAILURE
                        w.Task.cancel()
                    elif (w.Task.get_name() == S_SPIN and m_decode['name']=="STATUS" and m_decode['value']==S_MOTORFAILURE):
                        w.STATE = S_MOTORFAILURE
                        w.Task.cancel()
                else:
                    print(f"{time.ctime()} - ERROR MQTT [{m_decode['serial']}]:{m_decode['name']} => {m_decode['value']}")
            elif message.topic.matches(f"v1cdti/app/get/{student_id}/model-01/"):
                await publish_message(w, client, "app", "monitor", "STATUS", w.STATE)

async def main():
    n = 10
    w = WashingMachine(serial='SN-001')
    l = [WashingMachine(serial=f'SN-{i:03}') for i in range(n)]
    async with aiomqtt.Client("broker.hivemq.com") as client:
        li = [listen(w, client) for w in l]
        co = [CoroWashingMachine(w, client) for w in l]
        await asyncio.gather(*li,*co)

asyncio.run(main())