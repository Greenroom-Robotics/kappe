from typing import Any, Dict
from mcap_ros2._dynamic import _for_each_msgdef, TimeDefinition, read_message, encode_message
from mcap_ros2._vendor.rosidl_adapter.parser import MessageSpecification
from kappe.plugin import ConverterPlugin
from mcap.records import Channel

class UpgradeMissimInterfacesMsgVessels(ConverterPlugin):
    def __init__(self, *, some_var: int = 0) -> None:
        super().__init__()


    def convert(self, ros_msg: Any, channel: Channel) -> Any:
        # Get the necessary definitions
        msg_defs = get_definitions(self._schema.name, self._schema.data.decode())

        # Create a default encoded message to use as a template to create new messages 
        default_encoded_msg = encode_message(self._schema.name, msg_defs, {"vessels": []})
        default_encoded_vessel_msg = encode_message("missim_interfaces/Vessel", msg_defs, {})

        # Create a new message object with required methods
        msg = read_message(self._schema.name,msg_defs, default_encoded_msg)
        
        for vessel in ros_msg.vessels:
            # Get a template
            new_vessel = read_message("missim_interfaces/Vessel",msg_defs, default_encoded_vessel_msg)
            
            if (channel.metadata.get("topic_type_hash") == 'RIHS01_1d144744e73f20158b32965a8228356781167b06b17482f4a24cf3f19a2be481'):
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
            else:
                raise ValueError(f"No conversion from topic type hash: {channel.metadata.get('topic_type_hash')}")

            msg.vessels.append(new_vessel)
        
        return msg

    @property
    def output_schema(self) -> str:
        return 'missim_interfaces/msg/Vessels'


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