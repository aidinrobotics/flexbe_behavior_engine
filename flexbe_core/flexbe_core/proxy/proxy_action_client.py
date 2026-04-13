# Copyright 2024 Philipp Schillinger, Team ViGIR, Christopher Newport University
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
#    * Redistributions of source code must retain the above copyright
#      notice, this list of conditions and the following disclaimer.
#
#    * Redistributions in binary form must reproduce the above copyright
#      notice, this list of conditions and the following disclaimer in the
#      documentation and/or other materials provided with the distribution.
#
#    * Neither the name of the Philipp Schillinger, Team ViGIR, Christopher Newport University nor the names of its
#      contributors may be used to endorse or promote products derived from
#      this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.


"""A proxy for calling actions provides a single point for all state action interfaces."""
import uuid as uuid_lib
from functools import partial
from threading import Timer

from rclpy.action import ActionClient
from unique_identifier_msgs.msg import UUID as UUIDMsg

from flexbe_core.logger import Logger


class ProxyActionClient:
    """A proxy for calling actions."""

    _node = None
    _clients = {}
    _has_active_goal = {}
    _current_goal = {}
    _cancel_current_goal = {}

    _result = {}
    _result_status = {}
    _feedback = {}

    # ──────────────────────────────────────────────────────────────
    # Stale-result race guard (UUID-based).
    #
    # 문제:
    #   state1이 blend 조건으로 일찍 exit → 같은 topic에 state2가
    #   새 goal을 보냄 → state1의 지연 도착 result/feedback이
    #   state2의 슬롯을 오염시켜 state2가 즉시 'done'으로 오판.
    #
    # 해결:
    #   send_goal 시점에 UUID를 직접 생성해 goal_uuid로 넘기고,
    #   _active_uuid[topic]을 "현재 유효한 goal"의 진실의 원천으로 둔다.
    #   모든 콜백(goal accept / result / feedback)은 등록 시점의
    #   UUID를 클로저로 캡처해두었다가, 실행 시 _active_uuid와
    #   비교해서 일치하지 않으면 조용히 drop한다.
    # ──────────────────────────────────────────────────────────────
    _active_uuid = {}  # topic(str) → bytes (현재 유효한 goal uuid)

    @staticmethod
    def initialize(node):
        """Initialize ROS setup for proxy action client."""
        ProxyActionClient._node = node
        Logger.initialize(node)

    @staticmethod
    def shutdown():
        """Shuts this proxy down by reseting all action clients."""
        try:
            for topic, client in ProxyActionClient._clients.items():
                try:
                    ProxyActionClient._clients[topic] = None
                    ProxyActionClient._node.destroy_client(client)
                except Exception as exc:  # pylint: disable=W0703
                    Logger.error(f"Something went wrong during shutdown of proxy action client for {topic}!\n{str(exc)}")

            ProxyActionClient._result.clear()
            ProxyActionClient._result_status.clear()
            ProxyActionClient._feedback.clear()
            ProxyActionClient._cancel_current_goal.clear()
            ProxyActionClient._has_active_goal.clear()
            ProxyActionClient._current_goal.clear()
            ProxyActionClient._active_uuid.clear()
        except Exception as exc:  # pylint: disable=W0703
            Logger.error(f'Something went wrong during shutdown of proxy action clients!\n{ str(exc)}')

    def __init__(self, topics=None, wait_duration=10):
        """
        Initialize the proxy with optionally a given set of clients.

        @type topics: dictionary string - message class
        @param topics: A dictionay containing a collection of topic - message type pairs.

        @type wait_duration: int
        @param wait_duration: Defines how long to wait for each client in the
            given set to become available (if it is not already available).
        """
        if topics is not None:
            for topic, action_type in topics.items():
                ProxyActionClient.setupClient(topic, action_type, wait_duration)

    @classmethod
    def setupClient(cls, topic, action_type, wait_duration=10):
        """
        Set up an action client for calling it later.

        @type topic: string
        @param topic: The topic of the action to call.

        @type action_type: action type
        @param action_type: The type of Action for this action client.

        @type wait_duration: int
        @param wait_duration: Defines how long to wait for the given client if it is not available right now.
        """
        if topic not in ProxyActionClient._clients:
            ProxyActionClient._clients[topic] = ActionClient(ProxyActionClient._node, action_type, topic)
            ProxyActionClient._check_topic_available(topic, wait_duration)

        else:
            if action_type is not ProxyActionClient._clients[topic]._action_type:
                if action_type.__name__ == ProxyActionClient._clients[topic]._action_type.__name__:
                    # Logger.localinfo(f'Existing action client for {topic}'
                    #                  f' with same action type name, but different instance -  re-create  client!')

                    # Destroy the existing client in executor thread
                    client = ProxyActionClient._clients[topic]
                    ProxyActionClient._node.executor.create_task(ProxyActionClient.destroy_client, client, topic)

                    ProxyActionClient._clients[topic] = ActionClient(ProxyActionClient._node, action_type, topic)
                    ProxyActionClient._check_topic_available(topic, wait_duration)
                else:
                    raise TypeError("Trying to replace existing action client with different action type")

    @classmethod
    def send_goal(cls, topic, goal, wait_duration=0.0):
        """
        Call action on the given topic.

        @type topic: string
        @param topic: The topic to call.

        @type goal: action goal
        @param goal: The request to send to the action server.

        @type wait_duration: float seconds
        @param wait_duration: How long to wait for server
        """
        if not ProxyActionClient._check_topic_available(topic, wait_duration=wait_duration):
            raise ValueError(f'Cannot send goal for action client {topic}: Topic not available.')
        # reset previous results
        ProxyActionClient._result[topic] = None
        ProxyActionClient._result_status[topic] = None
        ProxyActionClient._feedback[topic] = None
        ProxyActionClient._cancel_current_goal[topic] = False
        ProxyActionClient._has_active_goal[topic] = True
        ProxyActionClient._current_goal[topic] = None

        if not isinstance(goal, ProxyActionClient._clients[topic]._action_type.Goal):
            if goal.__class__.__name__ == ProxyActionClient._clients[topic]._action_type.Goal.__name__:
                # This is the case if the same class is imported multiple times
                # To avoid rclpy TypeErrors, we will automatically convert to the base type
                # used in the original service/publisher clients
                new_goal = ProxyActionClient._clients[topic]._action_type.Goal()
                assert new_goal.__slots__ == goal.__slots__, f"Message attributes for {topic} do not match!"
                for attr in goal.__slots__:
                    setattr(new_goal, attr, getattr(goal, attr))
            else:
                raise TypeError(f"Invalid goal type {goal.__class__.__name__}"
                                f" (vs. {ProxyActionClient._clients[topic]._action_type.Goal.__name__}) for topic {topic}")
        else:
            # Same class definition instance as stored
            new_goal = goal

        # ── Generate a goal UUID and register it as the active one for this topic.
        #    이 한 줄이 "이전 goal의 모든 in-flight 콜백을 즉시 무효화" 하는 역할.
        goal_uuid_bytes = uuid_lib.uuid4().bytes
        goal_uuid_msg = UUIDMsg(uuid=list(goal_uuid_bytes))
        ProxyActionClient._active_uuid[topic] = goal_uuid_bytes

        # send goal
        ProxyActionClient._clients[topic].wait_for_server()
        future = ProxyActionClient._clients[topic].send_goal_async(
            new_goal,
            feedback_callback=partial(
                ProxyActionClient._feedback_callback,
                topic=topic,
                captured_uuid=goal_uuid_bytes,
            ),
            goal_uuid=goal_uuid_msg,
        )

        future.add_done_callback(
            partial(ProxyActionClient._done_callback, topic=topic, captured_uuid=goal_uuid_bytes)
        )

    @classmethod
    def _done_callback(cls, future, topic, captured_uuid):
        # Stale guard: 이 accept 콜백이 등록된 이후 새 send_goal이 발생했다면
        # captured_uuid != active_uuid 이 되어 이 goal은 더 이상 "현재 goal"이 아님.
        if ProxyActionClient._active_uuid.get(topic) != captured_uuid:
            # 이 goal의 result_future에 콜백을 붙이지 않으면, 이후 result가 와도
            # 아무도 구독하지 않으므로 _result[topic]이 오염되지 않는다.
            return

        ProxyActionClient._current_goal[topic] = future
        try:
            goal_handle = future.result()
        except Exception as exc:  # pylint: disable=W0703
            Logger.logerr(f"[{topic}] Failed to fetch goal handle: {exc}")
            ProxyActionClient._has_active_goal[topic] = False
            return

        if not goal_handle.accepted:
            Logger.logwarn(f"[{topic}] Goal was rejected by server")
            ProxyActionClient._has_active_goal[topic] = False
            return

        result_future = goal_handle.get_result_async()
        result_future.add_done_callback(
            partial(ProxyActionClient._result_callback, topic=topic, captured_uuid=captured_uuid)
        )

    @classmethod
    def _result_callback(cls, future, topic, captured_uuid):
        # Stale guard: result가 도착했지만 그 사이 더 새로운 goal이 나갔다면 drop.
        if ProxyActionClient._active_uuid.get(topic) != captured_uuid:
            return

        try:
            wrapped = future.result()
            ProxyActionClient._result[topic] = wrapped.result
            ProxyActionClient._result_status[topic] = wrapped.status
        except Exception as exc:  # pylint: disable=W0703
            Logger.logerr(f"[{topic}] Failed to fetch result: {exc}")
        ProxyActionClient._has_active_goal[topic] = False

    @classmethod
    def _feedback_callback(cls, feedback, topic, captured_uuid):
        # Stale guard: 이전 goal의 feedback이 새어들어오는 것을 차단.
        # (use_feedback_gate에서 transition_ready=True가 이전 state에서 새는 것 방지)
        if ProxyActionClient._active_uuid.get(topic) != captured_uuid:
            return
        ProxyActionClient._feedback[topic] = feedback

    @classmethod
    def is_available(cls, topic):
        """
        Check if the client and server for the given action topic is available.

        @type topic: string
        @param topic: The topic of interest.
        """
        client = ProxyActionClient._clients.get(topic)
        if client is None:
            Logger.logerr("Action client '%s' is not yet registered, need to add it first!" % topic)
            return False

        return client.server_is_ready()

    @classmethod
    def has_result(cls, topic):
        """
        Check if the given action call already has a result.

        @type topic: string
        @param topic: The topic of interest.
        """
        return ProxyActionClient._result.get(topic) is not None

    @classmethod
    def get_result(cls, topic):
        """
        Return the result message of the given action call.

        @type topic: string
        @param topic: The topic of interest.
        """
        return ProxyActionClient._result.get(topic)

    @classmethod
    def remove_result(cls, topic):
        """
        Remove the latest results of the given action call.

        @type topic: string
        @param topic: The topic of interest.
        """
        ProxyActionClient._result[topic] = None
        ProxyActionClient._result_status[topic] = None

    @classmethod
    def has_feedback(cls, topic):
        """
        Check if the given action call has any feedback.

        @type topic: string
        @param topic: The topic of interest.
        """
        return ProxyActionClient._feedback.get(topic) is not None

    @classmethod
    def get_feedback(cls, topic):
        """
        Return the latest feedback message of the given action call.

        @type topic: string
        @param topic: The topic of interest.
        """
        return ProxyActionClient._feedback.get(topic)

    @classmethod
    def remove_feedback(cls, topic):
        """
        Remove the latest feedback message of the given action call.

        @type topic: string
        @param topic: The topic of interest.
        """
        ProxyActionClient._feedback[topic] = None

    @classmethod
    def get_state(cls, topic):
        """
        Determine the current actionlib state of the given action topic.

        A list of possible states is defined in actionlib_msgs/GoalStatus.

        @type topic: string
        @param topic: The topic of interest.
        """
        return ProxyActionClient._result_status.get(topic)

    @classmethod
    def is_active(cls, topic):
        """
        Determine if an action request is already being processed on the given topic.

        @type topic: string
        @param topic: The topic of interest.
        """
        return ProxyActionClient._has_active_goal.get(topic, False)

    @classmethod
    def cancel(cls, topic):
        """
        Cancel the current action call on the given action topic.

        @type topic: string
        @param topic: The topic of interest.
        """
        current_goal = ProxyActionClient._current_goal.get(topic)
        if current_goal is not None:
            current_goal.result().cancel_goal()

        ProxyActionClient._cancel_current_goal[topic] = True
        ProxyActionClient._current_goal[topic] = None
        # active_uuid도 비워 이후 도착할 콜백을 자연스럽게 drop시킴
        ProxyActionClient._active_uuid[topic] = None

    @classmethod
    def _check_topic_available(cls, topic, wait_duration=0.1):
        """
        Check whether a topic is available.

        @type topic: string
        @param topic: The topic of the action.

        @type wait_duration: int
        @param wait_duration: Defines how long to wait for the given client if it is not available right now.
        """
        client = ProxyActionClient._clients.get(topic)
        if client is None:
            Logger.logerr("Action client '%s' is not yet registered, need to add it first!" % topic)
            return False

        if wait_duration > 2.0:
            tmr = Timer(.5, ProxyActionClient._print_wait_warning, [topic])
            tmr.start()

        available = client.wait_for_server(wait_duration)

        if wait_duration > 2.0:
            try:
                tmr.cancel()
            except Exception:  # pylint: disable=W0703
                pass

        if not available:
            Logger.logerr(f"Action client/server '{topic}' is not available - timed out after {wait_duration:.3f} seconds!")
            return False

        return True

    @classmethod
    def _print_wait_warning(cls, topic):
        Logger.logwarn(f"Waiting for action client/server for '{topic}'")

    @classmethod
    def destroy_client(cls, client, topic):
        """Handle client destruction from within the executor threads."""
        try:
            # if ProxyActionClient._node.destroy_client(client):
            #     Logger.localinfo(f'Destroyed the proxy action client for {topic} ({id(client)})!')
            # else:
            #     Logger.localwarn(f'Some issue destroying the proxy action client for {topic}!')
            del client
        except Exception as exc:  # pylint: disable=W0703
            Logger.error("Something went wrong destroying proxy action client"
                         f" for {topic}!\n  {type(exc)} - {str(exc)}")