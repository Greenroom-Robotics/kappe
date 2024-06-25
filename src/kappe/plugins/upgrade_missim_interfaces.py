'''
This plugin can be used with Kappe to 'upgrade' interfaces to the newest version. Before you can use the plugin you must have the
newest interface package colcon built. You then want to provide the path to the msg folder containing the desired interface, which 
is usually in the install > package_name > share > package_name > msg folder.

PLUGIN CONFIG FILE STRUCTURE:

msg_folders: ['/home/path/to/your/msg/folder']
plugins:
  - name: upgrade.UpgradeInterfaceMsgs
    input_topic: /old-topic_name-you-want-to-upgrade
    output_topic: /new-topic_name-you-want-to-upgrade # NOTE Can be the same name as the input topic
    settings:
      interface_type: new_interface_package_name/msg/new_msg_name # i.e. car_interfaces/msg/Car
      interface_definition: new_interface_package_name/new_msg_name # i.e. car_interfaces/Car
      old_type_hash: old_topic_type_hash
      new_type_hash: new_topic_type_hash

EXAMPLE PLUGIN CONFIG FILE:

msg_folders: ['/ros_ws/install/car_interfaces/share/car_interfaces/msg']
plugins:
  - name: upgrade.UpgradeInterfaceMsgs
    input_topic: /car_info
    output_topic: /car_info 
    settings:
      interface_type: car_interfaces/msg/Car
      interface_definition: car_interfaces/Car
      old_type_hash: RIHS01_3e8dc9e2a1254b4b8a1b534db3e6172a8b14f10b5a36e3c3b3ec4f34a8e6b5a2
      new_type_hash: RIHS01_7b8ef0b1c2a94a4ba8d8c5f603c8a2a2b5c4f10b5a36e3c3b3ec4f34a8e6b5b3
'''

from typing import Any, Dict, List
from mcap_ros2._dynamic import _for_each_msgdef, TimeDefinition, read_message, encode_message
from mcap_ros2._vendor.rosidl_adapter.parser import MessageSpecification
from kappe.plugin import ConverterPlugin
from mcap.records import Channel

class UpgradeMissimInterfaceMsgs(ConverterPlugin):
    def __init__(self, *, interface_type: str, interface_definition: str, old_type_hash: str, new_type_hash: str) -> None:
        super().__init__()
        self.interface_type = interface_type
        self.interface_definition = interface_definition
        self.old_type_hash = old_type_hash
        self.new_type_hash = new_type_hash
        self.checked_type_hash = False    


    def convert(self, ros_msg: Any, channel: Channel) -> Any:
        # Get the definitions from the import msgs
        msg_defs = get_definitions(self._schema.name, self._schema.data.decode())

        # Get the target message definition
        target_msg_def = msg_defs.get(self.interface_definition)

        # Create a default encoded message to use as a template to create new messages 
        default_encoded_msg = encode_message(self._schema.name, msg_defs, target_msg_def)

        # Create a new message object with required methods
        msg = read_message(self._schema.name,msg_defs, default_encoded_msg)

        # Check supplied mcap and topic contains the correct topic type hash  
        if (channel.metadata.get("topic_type_hash") != self.old_type_hash and not self.checked_type_hash):
            raise ValueError(f"MCAP file or topic does not contain the given topic type hash: {channel.metadata.get('topic_type_hash')}")
        else:
            self.checked_type_hash = True
        
        '''
        EXAMPLE CONVERSION METHOD

        ***The following is an example of an 'old' custom interface:

        # Name of the car
        string name

        # The speed of the car in m/s
        float32 speed

        # Waypoints of where the car has travelled
        geographic_msgs/GeoPoint[] waypoints

        ***This interface has been updated to include a new entry, acceleration:

        # Name of the car
        string name

        # The speed of the car in m/s
        float32 speed

        # Waypoints of where the car has travelled
        geographic_msgs/GeoPoint[] waypoints

        # The acceleration of the car in m/s^2
        float32 acceleration

        ***This is how the conversion would be implemented in this file. NOTE that the 'acceleration' field does not NEED to be explicitly defined as it will initialise to an 'empty' type:

        new_car = {
            "name": car.name,
            "speed": car.speed,
            for waypoint in car.waypoints:
                "waypoints": waypoint
            "acceleration": 0.0
        }
        msg.cars.append(new_car)
        
        IMPLEMENT YOUR CONVERSION BELOW'''
        
        for vessel in ros_msg.vessels:
            new_vessel = {
                        "name": vessel.name,
                        "components": {
                            "ais": vessel.components.ais,
                            "ais_signature": vessel.components.ais_signature,
                            "arpa": vessel.components.arpa,
                            "arpa_signature": vessel.components.arpa_signature,
                            "cameras": vessel.components.cameras,
                            "geopose": vessel.components.geopose,
                            "thrusters": vessel.components.thrusters,
                            "vessel_spawner": vessel.components.vessel_spawner,
                            "velocity": {
                                "name": vessel.components.velocity.name,
                                "initial": vessel.components.velocity.initial,
                                "output_topic": vessel.components.velocity.output_topic,
                            }
                        },
                        "base_link_frame_id": vessel.base_link_frame_id,
                        "model_name": vessel.model_name,
                        "sim_params": vessel.sim_params,
            }
            msg.vessels.append(new_vessel)

        channel.metadata["topic_type_hash"] = self.new_type_hash
        return msg

    @property
    def output_schema(self) -> str:
        return self.interface_type


def get_definitions(schema_name: str, schema_text: str) -> Dict[str, MessageSpecification]:
    msgdefs: Dict[str, MessageSpecification] = {
        "builtin_interfaces/Time": TimeDefinition,
        "builtin_interfaces/Duration": TimeDefinition,
    }

    def handle_msgdef(
        cur_schema_name: str, short_name: str, msgdef: MessageSpecification
    ):
        # Add the message definition to the dictionary
        msgdefs[cur_schema_name] = msgdef
        msgdefs[short_name] = msgdef


    _for_each_msgdef(schema_name, schema_text, handle_msgdef)
    return msgdefs


# from typing import Any, Dict
# from mcap_ros2._dynamic import _for_each_msgdef, TimeDefinition, read_message, encode_message
# from mcap_ros2._vendor.rosidl_adapter.parser import MessageSpecification
# from kappe.plugin import ConverterPlugin
# from mcap.records import Channel

# class UpgradeMissimInterfacesMsgVessels(ConverterPlugin):
#     def __init__(self, *, some_var: int = 0) -> None:
#         super().__init__()


#     def convert(self, ros_msg: Any, channel: Channel) -> Any:
#         # Get the necessary definitions
#         msg_defs = get_definitions(self._schema.name, self._schema.data.decode())

#         # Create a default encoded message to use as a template to create new messages 
#         default_encoded_msg = encode_message(self._schema.name, msg_defs, {"vessels": []})
#         default_encoded_vessel_msg = encode_message("missim_interfaces/Vessel", msg_defs, {})

#         # Create a new message object with required methods
#         msg = read_message(self._schema.name,msg_defs, default_encoded_msg)
        
#         for vessel in ros_msg.vessels:
#             # Get a template
#             new_vessel = read_message("missim_interfaces/Vessel",msg_defs, default_encoded_vessel_msg)
            
#             if (channel.metadata.get("topic_type_hash") == 'RIHS01_1d144744e73f20158b32965a8228356781167b06b17482f4a24cf3f19a2be481'):
#                 new_vessel = {
#                     "name": vessel.name,
#                     "components": {
#                         "ais": vessel.components.ais,
#                         "ais_signature": vessel.components.ais_signature,
#                         "arpa": vessel.components.arpa,
#                         "arpa_signature": vessel.components.arpa_signature,
#                         "cameras": vessel.components.cameras,
#                         "geopose": vessel.components.geopose,
#                         "thrusters": vessel.components.thrusters,
#                         "vessel_spawner": vessel.components.vessel_spawner,
#                         "velocity": {
#                             "name": vessel.components.velocity.name,
#                             "initial": vessel.components.velocity.initial,
#                             "output_topic": vessel.components.velocity.output_topic,
#                         }
#                     },
#                     "base_link_frame_id": vessel.base_link_frame_id,
#                     "model_name": vessel.model_name,
#                     "sim_params": vessel.sim_params,
#                 }
#             else:
#                 raise ValueError(f"No conversion from topic type hash: {channel.metadata.get('topic_type_hash')}")

#             msg.vessels.append(new_vessel)
        
#         return msg

#     @property
#     def output_schema(self) -> str:
#         return 'missim_interfaces/msg/Vessels'


# def get_definitions(schema_name: str, schema_text: str) -> Dict[str, MessageSpecification]:
#     msgdefs: Dict[str, MessageSpecification] = {
#         "builtin_interfaces/Time": TimeDefinition,
#         "builtin_interfaces/Duration": TimeDefinition,
#     }

#     def handle_msgdef(
#         cur_schema_name: str, short_name: str, msgdef: MessageSpecification
#     ):
#         # Add the message definition to the dictionary
#         msgdefs[cur_schema_name] = msgdef
#         msgdefs[short_name] = msgdef


#     _for_each_msgdef(schema_name, schema_text, handle_msgdef)
#     return msgdefs