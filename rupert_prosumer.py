"""
Description: Parent module largely used to ensure a consistent
interface for communicating with kafka
"""

import json
import time
from pathlib import Path
from beartype import beartype
from confluent_kafka import Consumer, KafkaException
from confluent_kafka.aio import AIOProducer
from confluent_kafka.admin import AdminClient, NewTopic
from rupert_config import RupertConfig
from rupert_logger import RupertLogger

class RupertProsumer():
	"""
		Description: Parent class used by rupert prosumers.
		Responsible for:
			1. Basic constructor for starting prosumers.
			2. Initiates Kafka consumer
			3. Contains method to push to Kafka as producer
	"""
	@beartype
	def __init__(self, config_file: str) -> None:
		"""
			Constructor for Rupert prosumer classes.
			Responsible for:
				1. Loading config.
				2. Initialize the topic consumer.
			Requires:
				config_file - Path to the rupert prosumer setting file
		"""
		self.config_file = config_file
		self.config = {}
		self.__load_config()
		self.__validate_config()
		self.logger = None
		self.consumer = None
		self.close_consumer = False
		self.sensitive_trace_warning_logged = False

	@beartype
	def listen(self, topic: str) -> None:
		"""
		Consume messages from a Kafka topic and process them.

		Connects to Kafka using configured connection and consumer settings,
		subscribes to the specified topic, and enters a message loop. Calls
		process_event() for each message received. Exits when stop() is called.

		Args:
			topic: Topic key from kafka.topics config, or a raw Kafka topic name.

		Raises:
			RuntimeError: If Kafka connection or consumer setup fails.

		Note:
			This method loops indefinitely until stop() is called. Call stop()
			from within process_event() or from another thread.
		"""

		logging_cfg = dict(self.config['logging'])
		log_directory = logging_cfg.get('log_directory')
		if log_directory:
			logging_cfg['log_file'] = str(Path(log_directory).expanduser() / f"consumer.{topic}.log")
		else:
			logging_cfg['log_file'] = f"consumer.{topic}.log"
		self.logger = RupertLogger(logging_cfg)
		try:
			topic_name = self.config['kafka']['topics'].get(topic, topic)
			self.consumer = Consumer(self.config['kafka']['connection'] | self.config['kafka']['consumer'])
			self.consumer.subscribe([topic_name])
		except (AttributeError, KeyError, TypeError, ValueError) as e:
			message = f"Kafka consumer error: {e}"
			self.__log_error(message)
			raise RuntimeError(message) from e

		consecutive_errors = 0
		max_consecutive_errors = 5
		while True:
			if self.close_consumer:
				self.consumer.close()
				return
			try:
				msg = self.consumer.poll(1.0)
			except (KafkaException, RuntimeError,TypeError) as e:
				consecutive_errors += 1
				error_message = f"Kafka connection error: (attempt {consecutive_errors}) {e}"
				self.__log_error(error_message)

				if consecutive_errors >= max_consecutive_errors:
					message = "Max consecutive errors reached. Exiting consumer loop."
					self.__log_error(message)
					raise RuntimeError(message)
				continue
			if msg is None:
				consecutive_errors = 0
				continue
			elif msg.error():
				kafka_error = msg.error()
				consecutive_errors += 1

				error_message = (
					f"Kafka message error: (attempt {consecutive_errors}) {kafka_error}"
				)
				self.__log_error(error_message)

				is_fatal_error = False
				try:
					is_fatal_error = bool(kafka_error.fatal())
				except (AttributeError, TypeError, RuntimeError):
					is_fatal_error = False

				if is_fatal_error:
					fatal_message = f"Fatal Kafka message error encountered: {kafka_error}"
					self.__log_error(fatal_message)
					raise RuntimeError(fatal_message)

				if consecutive_errors >= max_consecutive_errors:
					max_error_message = "Max consecutive message errors reached. Exiting consumer loop."
					self.__log_error(max_error_message)
					raise RuntimeError(max_error_message)

				time.sleep(min(consecutive_errors, 5))
				continue

			consecutive_errors = 0
			try:
				self.process_event(msg)
			except (UnicodeDecodeError, TypeError) as e:
				consecutive_errors += 1
				error_message = f"Error processing event: (attempt {consecutive_errors}) {e}"
				self.__log_error(error_message)

	@beartype
	def process_event(self, consumer_message) -> None:
		"""
			Description: Each Rupert prosumer should override this method. This default
				implementation is primarily for Kafka connectivity testing.
			Responsible for:
				1. Converts the message value to a string.
				2. Logs message metadata.
				3. Logs redacted payload details only when explicitly enabled.
				4. Stops the consumer loop.
			Requires:
				consumer_message
		"""
		try:
			topic_name = consumer_message.topic()
			message_bytes = consumer_message.value()
			decoded_message = message_bytes.decode('utf-8')
			self.logger.success("Successfully received an event.")
			self.logger.trace(
				f"RECEIVED MESSAGE metadata: topic={topic_name}, bytes={len(message_bytes)}"
			)
			if self.__allow_sensitive_trace_payloads():
				self.logger.trace(
					f"RECEIVED MESSAGE payload(redacted): "
					f"{self.__redact_trace_payload(decoded_message, topic_name)}"
				)
		except UnicodeDecodeError as e:
			message = f"Message decoding error: {e}"
			self.__log_error(message)
			try:
				payload_size = 0 if message_bytes is None else len(message_bytes)
				self.logger.trace(f"Message decode metadata: bytes={payload_size}")
				if self.__allow_sensitive_trace_payloads():
					self.logger.trace(
						f"Message bytes payload(redacted): "
						f"{self.__redact_trace_payload(message_bytes, topic_name)}"
					)
			except (AttributeError, TypeError, ValueError, RuntimeError) as e:
				message = f"Exception error during testing: {e}"
				self.__log_error(message)
		except (AttributeError, TypeError) as e:
			message = f"Error processing message: {e}"
			self.__log_error(message)
		time.sleep(10)
		self.stop()

	@beartype
	def reload(self):
		"""
			Reloads the rupert prosumer
				1. Reloads the configuration
		"""
		self.__load_config()

	@beartype
	async def send(self, topic: str, event_bytes: bytes) -> None:
		"""
		 Description: Sends a byte array to Kafka as a producer
		 Responsible for:
	 		1. Send event bytes to topic
	 	 Requires:
 			1. topic - Name of the topic to send message/event to (string)
			2. event_bytes - array of bytes
		Raises:
			BufferError - if the internal producer message queue is full
			KafkaException - for other errors, see exception code
			NotImplementedError - if timestamp is specified without underlying library support.
	 """
		if self.logger is None:
			try:
				logging_cfg = dict(self.config['logging'])
				log_directory = logging_cfg.get('log_directory')
				if log_directory:
					logging_cfg['log_file'] = str(Path(log_directory).expanduser() / f"producer.{topic}.log")
				else:
					logging_cfg['log_file'] = f"producer.{topic}.log"
				self.logger = RupertLogger(logging_cfg)
			except (KeyError, TypeError, ValueError):
				self.logger = None

		producer = None
		try:
			topic_name = self.config['kafka']['topics'].get(topic, topic)
			producer = AIOProducer(self.config['kafka']['connection'])
			delivery_future = await producer.produce(topic_name, event_bytes)
			await delivery_future
			await producer.poll(2.0)
			await producer.flush()
			if self.logger is not None:
				self.logger.success("Successfully sent an event.")
		except BufferError as e:
			self.__log_error(f"Producer buffer error: {e}")
			raise
		except (KeyError, TypeError, ValueError) as e:
			self.__log_error(f"Producer configuration error: {e}")
			raise
		except (KafkaException, RuntimeError) as e:
			self.__log_error(f"Kafka producer error: {e}")
			raise
		finally:
			if producer is not None:
				await producer.close()

	@beartype
	def serialize_to_json(self, event: dict) -> str:
		"""
			Description: Serializes a dictionary to a JSON string
			Responsible for:
				1. Serializes a dictionary to JSON
				2. Returns the JSON string
		"""
		try:
			event_json = json.dumps(event)
			return event_json
		except (TypeError, ValueError) as e:
			self.__log_error(f"JSON serialization error: {e}")
			if self.logger is not None:
				self.logger.trace(f"Event serialization metadata: type={type(event).__name__}")
				if self.__allow_sensitive_trace_payloads():
					self.logger.trace(
						f"Event serialization payload(redacted): {self.__redact_trace_payload(event)}"
					)
			raise

	@beartype
	def stop(self) -> None:
		"""Signals the consumer loop to stop and logs shutdown."""
		self.close_consumer = True
		if self.logger is not None:
			self.logger.warning("Shutting down Rupert Prosumer...")

	def __log_error(self, message: str) -> None:
		"""Logs errors when logger is configured."""
		if self.logger is not None:
			self.logger.error(message)

	def __allow_sensitive_trace_payloads(self) -> bool:
		"""True when sensitive trace payload logging is explicitly enabled."""
		logging_cfg = self.config.get('logging', {})
		allow_sensitive = bool(logging_cfg.get('allow_sensitive_trace_payloads', False))
		if allow_sensitive and self.logger is not None and not self.sensitive_trace_warning_logged:
			self.logger.warning(
				"Sensitive trace payload logging is enabled. Use only for urgent debugging/testing."
			)
			self.sensitive_trace_warning_logged = True
		return allow_sensitive

	def __redact_trace_payload(self, payload, topic_name: str | None = None) -> str:
		"""Returns a redacted/safelisted representation for trace payload logging."""
		redaction_tokens = (
			'password',
			'passphrase',
			'secret',
			'token',
			'api_key',
			'apikey',
			'authorization',
			'auth',
			'session',
			'cookie',
			'key',
		)

		def sanitize(value):
			if isinstance(value, dict):
				sanitized = {}
				for item_key, item_value in value.items():
					key_text = str(item_key).lower()
					if any(token in key_text for token in redaction_tokens):
						sanitized[item_key] = '<redacted>'
					else:
						sanitized[item_key] = sanitize(item_value)
				return sanitized
			if isinstance(value, list):
				return [sanitize(item) for item in value]
			if isinstance(value, tuple):
				return [sanitize(item) for item in value]
			return value

		def select_safelisted_fields(value):
			if not isinstance(value, dict):
				return None
			allowed_fields = self.__get_trace_payload_allowed_fields(topic_name)
			if allowed_fields is None:
				return None
			return {
				field_name: value[field_name]
				for field_name in allowed_fields
				if field_name in value
			}

		if isinstance(payload, bytes):
			payload_text = payload.decode('utf-8', errors='replace')
		elif isinstance(payload, str):
			payload_text = payload
		else:
			payload_text = json.dumps(payload, default=str)

		try:
			payload_obj = json.loads(payload_text)
			safelisted_payload = select_safelisted_fields(payload_obj)
			if safelisted_payload is None:
				return "<payload suppressed; configure logging.trace_payload_allowed_fields>"
			return json.dumps(sanitize(safelisted_payload), default=str)
		except (TypeError, ValueError):
			return f"<non-json payload; length={len(payload_text)}>"

	def __get_trace_payload_allowed_fields(self, topic_name: str | None) -> set[str] | None:
		"""Returns configured safelisted trace payload fields for a topic, if available."""
		logging_cfg = self.config.get('logging', {})
		safelist_cfg = logging_cfg.get('trace_payload_allowed_fields')
		if not isinstance(safelist_cfg, dict):
			return None

		topic_fields = safelist_cfg.get(topic_name) if topic_name is not None else None
		default_fields = safelist_cfg.get('*')

		selected_fields = topic_fields if topic_fields is not None else default_fields
		if not isinstance(selected_fields, list):
			return None

		return {field for field in selected_fields if isinstance(field, str)}

	## Private methods, best not to override anything beyond this point

	@beartype
	def __load_config(self) -> None:
		"""
			Description: Parent class used by other rupert prosumers.
			Responsible for:
				1. Loading config
			Requires:
				config_file - Path to the rupert prosumer setting file
		"""
		with open(self.config_file, 'r', encoding='utf-8') as config:
			config_json = config.read()
		self.config = json.loads(config_json)

	@beartype
	def __validate_config(self) -> None:
		"""
			Description: Validates the loaded config
			Responsible for:
				1. Ensuring required keys exist in the config
		"""
		required_keys = ['kafka', 'logging']
		for key in required_keys:
			if key not in self.config:
				raise ValueError(f"Missing required config key: '{key}'")

		logging_cfg = self.config.get('logging', {})
		if not isinstance(logging_cfg, dict):
			raise TypeError("'logging' must be a dictionary")
		allow_sensitive = logging_cfg.get('allow_sensitive_trace_payloads')
		if allow_sensitive is not None and not isinstance(allow_sensitive, bool):
			raise TypeError("'logging.allow_sensitive_trace_payloads' must be a boolean")
		trace_payload_fields = logging_cfg.get('trace_payload_allowed_fields')
		if trace_payload_fields is not None:
			if not isinstance(trace_payload_fields, dict):
				raise TypeError("'logging.trace_payload_allowed_fields' must be a dictionary")
			for topic_key, allowed_fields in trace_payload_fields.items():
				if not isinstance(topic_key, str):
					raise TypeError("'logging.trace_payload_allowed_fields' keys must be strings")
				if not isinstance(allowed_fields, list) or not all(
					isinstance(field, str) for field in allowed_fields
				):
					raise TypeError(
						"'logging.trace_payload_allowed_fields' values must be lists of strings"
					)

		kafka_cfg = self.config.get('kafka', {})
		required_kafka_keys = ['connection', 'consumer', 'topics']
		for key in required_kafka_keys:
			if key not in kafka_cfg:
				raise ValueError(f"Missing required Kafka config key: 'kafka.{key}'")

		if not isinstance(kafka_cfg['connection'], dict):
			raise TypeError("'kafka.connection' must be a dictionary")
		if not isinstance(kafka_cfg['consumer'], dict):
			raise TypeError("'kafka.consumer' must be a dictionary")
		if not isinstance(kafka_cfg['topics'], dict):
			raise TypeError("'kafka.topics' must be a dictionary")

class RupertProsumerAdminClient():
	"""
		Description: Responsible for managing Kafka topics for Rupert Prosumer
		Responsible for:
			1. Creating topics
			2. Deleting topics
			3. Retrieving topics
	"""
	@beartype
	def __init__(self, config_file: str) -> None:
		"""
			Description: Constructor for initializing Kafka
			Responsible for:
				1. Validate and load Kafka configuration.
				2. Load config.
				3. Initialize Kafka admin client state.
		"""

		self.config_file = config_file
		self.config = RupertConfig(self.config_file).config
		self.logger = None
		try:
			logging_cfg = dict(self.config.get('logging', {}))
			log_directory = logging_cfg.get('log_directory')
			if log_directory:
				logging_cfg['log_file'] = str(Path(log_directory).expanduser() / "admin.kafka.log")
			elif 'log_file' not in logging_cfg:
				logging_cfg['log_file'] = "admin.kafka.log"
			self.logger = RupertLogger(logging_cfg)
		except (TypeError, ValueError):
			self.logger = None

		try:
			self.consumer_cfg = self.config['kafka']['connection'] | self.config['kafka']['consumer']
			self.admin_client = AdminClient(self.config['kafka']['connection'])
		except KeyError as e:
			message = f"Missing Kafka config key: {e}"
			self.__log_error(message)
			raise RuntimeError(message) from e
		except TypeError as e:
			message = f"Type error in Kafka config: {e}"
			self.__log_error(message)
			raise RuntimeError(message) from e
		except ValueError as e:
			message = f"Value error in Kafka config: {e}"
			self.__log_error(message)
			raise RuntimeError(message) from e
		except KafkaException as e:
			message = f"Kafka exception: {e}"
			self.__log_error(message)
			raise RuntimeError(message) from e

	@beartype
	def get_topics(self) -> dict:
		"""
			Description: Return Kafka topic metadata from the cluster.
			Responsible for:
				1. Retrieve topic metadata.
		"""
		consumer = None
		try:
			consumer = Consumer(self.config['kafka']['connection'] | self.config['kafka']['consumer'])
			return consumer.list_topics(timeout=10.0).topics
		except KafkaException as e:
			message = f"Kafka exception: {e}"
			self.__log_error(message)
			raise RuntimeError(message) from e
		except TypeError as e:
			message = f"Type error in Kafka config: {e}"
			self.__log_error(message)
			raise RuntimeError(message) from e
		except ValueError as e:
			message = f"Value error in Kafka config: {e}"
			self.__log_error(message)
			raise RuntimeError(message) from e
		except RuntimeError as e:
			message = f"Runtime error in Kafka client: {e}"
			self.__log_error(message)
			raise RuntimeError(message) from e
		finally:
			if consumer is not None:
				consumer.close()

	@beartype
	def initialize(self) -> None:
		"""
			Description: Initializes Kafka topics
			Responsible for:
				1. Creates topics
		"""

		topics = []
		if self.logger is not None:
			self.logger.info('Creating Topics')
		for topic in self.config['kafka']['topics']:
			if self.logger is not None:
				self.logger.info(f"  {self.config['kafka']['topics'][topic]}")
			topics.append(NewTopic(topic=self.config['kafka']['topics'][topic],
				num_partitions=1, replication_factor=1))
		try:
			topic_futures = self.admin_client.create_topics(new_topics=topics, validate_only=False)
			for topic, future in topic_futures.items():
				future.result(timeout=10.0)
		except KafkaException as e:
			message = f"Kafka exception: {e}"
			self.__log_error(message)
			raise RuntimeError(message) from e
		except TypeError as e:
			message = f"Type error in Kafka config: {e}"
			self.__log_error(message)
			raise RuntimeError(message) from e
		except ValueError as e:
			message = f"Value error in Kafka config: {e}"
			self.__log_error(message)
			raise RuntimeError(message) from e
		except RuntimeError as e:
			message = f"Runtime error in Kafka client: {e}"
			self.__log_error(message)
			raise RuntimeError(message) from e

	@beartype
	def reset(self) -> None:
		"""
			Description: Resets Kafka topics
			Responsible for:
				1. Deletes topics
		"""
		topics = []
		if self.logger is not None:
			self.logger.info('Deleting Topics')
		for topic in self.config['kafka']['topics']:
			if self.logger is not None:
				self.logger.info(f"  {self.config['kafka']['topics'][topic]}")
			topics.append(self.config['kafka']['topics'][topic])
		try:
			topic_futures = self.admin_client.delete_topics(topics)
			for topic, future in topic_futures.items():
				future.result(timeout=10.0)
		except KafkaException as e:
			message = f"Kafka exception: {e}"
			self.__log_error(message)
			raise RuntimeError(message) from e
		except TypeError as e:
			message = f"Type error in Kafka config: {e}"
			self.__log_error(message)
			raise RuntimeError(message) from e
		except ValueError as e:
			message = f"Value error in Kafka config: {e}"
			self.__log_error(message)
			raise RuntimeError(message) from e
		except RuntimeError as e:
			message = f"Runtime error in Kafka client: {e}"
			self.__log_error(message)
			raise RuntimeError(message) from e

	def __log_error(self, message: str) -> None:
		"""Logs errors when logger is configured."""
		if self.logger is not None:
			self.logger.error(message)
