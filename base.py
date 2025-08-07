import asyncio
import contextlib
import datetime
import functools
import logging
import logging.handlers
import pathlib
import signal
import sys
import time
from asyncio import CancelledError

import asyncpg
import sqlalchemy as sa
import sqlalchemy.ext.asyncio as asa


def utcnow() -> datetime.datetime:
	return datetime.datetime.now(datetime.timezone.utc)


class Shutdown(Exception): pass


class ColorFormatter(logging.Formatter):
	BLACK, RED, GREEN, YELLOW, BLUE, MAGENTA, CYAN, WHITE = range(8)
	COLORS = {
		'DEBUG': f'\033[0;{30 + BLUE}m',
		'INFO': f'\033[0;{30 + CYAN}m',
		'WARNING': f'\033[1;{30 + YELLOW}m',
		'ERROR': f'\033[1;{30 + RED}m',
		'CRITICAL': f'\033[1;{30 + RED}m',
	}

	def format(self, record):
		line = super().format(record)
		if (color := self.COLORS.get(record.levelname)) is not None:
			line = color + line + '\033[0m'
		return line


class Helper:
	def __init__(self, base: 'Base'):
		self.base = base  # type: Base
		self.log = logging.getLogger(self._logging_name())
		self.name = None  # type: str|None

	def _logging_name(self):
		return 'vrc_lc'

	async def gather(self, tasks: 'list[Task]', return_exceptions=False):
		await asyncio.gather(*(t.wait() for t in tasks), return_exceptions=return_exceptions)

	def check_shutdown(self):
		if self.base.shutdown_requested.is_set():
			raise Shutdown()

	# # #

	async def db_conn(self, stack: 'contextlib.AsyncExitStack') -> 'asa.AsyncConnection':
		conn = self.base.db_engine.connect()  # type: asa.AsyncConnection|None
		await stack.enter_async_context(self.base.db_sem)
		await stack.enter_async_context(conn)
		return conn

	def db_t_translations(self) -> 'sa.Table':
		return self.base.db_meta.tables['translations']

	def db_t_latest_translations(self) -> 'sa.Table':
		return self.base.db_meta.tables['latest_translations']

	def db_t_suggestions(self) -> 'sa.Table':
		return self.base.db_meta.tables['suggestions']

	def db_t_latest_suggestions(self) -> 'sa.Table':
		return self.base.db_meta.tables['latest_suggestions']

	async def db_refresh_views(self, *views: 'sa.Table'):
		self.check_shutdown()  # todo should actually leave?
		names = list(t.name for t in views)
		self.info(f"Refreshing DB materialized views {names!r}...")
		begin = time.monotonic()
		async with contextlib.AsyncExitStack() as stack:
			conn = await self.base.db_conn(stack)
			for name in names:
				view_name = sa.quoted_name(name, quote=True)
				statement = sa.text(f"REFRESH MATERIALIZED VIEW {view_name}")
				await conn.execute(statement)
				await conn.commit()
		self.info(f"Refreshed DB materialized views {names!r} in {self.span(begin)}.")

	# # #

	def span(self, monotonic: 'float') -> 'datetime.timedelta':
		return datetime.timedelta(seconds=time.monotonic() - monotonic)

	def info(self, message, **kwargs):
		self.log.info(f"[{self.name}] {message}", **kwargs)

	def warning(self, message, **kwargs):
		self.log.warning(f"[{self.name}] {message}", **kwargs)

	def warning_exc(self, message: 'str', exc: 'BaseException', exc_info=False):
		self.log.warning(f"[{self.name!r}] {message}: {type(exc).__name__}: {exc}", exc_info=exc if exc_info else None)

	def error(self, message, **kwargs):
		self.log.error(f"[{self.name!r}] {message}", **kwargs)

	def error_exc(self, message: 'str', exc: 'BaseException', exc_info=True):
		self.log.error(f"[{self.name!r}] {message}: {type(exc).__name__}: {exc}", exc_info=exc if exc_info else None)

	def error_raise(self, exc_t: 'type[BaseException]', message: 'str'):
		self.log.error(f"[{self.name!r}] {message}")
		raise exc_t(message)


class Task(Helper):
	SEM = None  # type: asyncio.Semaphore|None

	def __init__(self, base: 'Base'):
		super().__init__(base)
		self._task = None  # type: asyncio.Task|None
		self.log = base.log
		self._stack = contextlib.AsyncExitStack()
		self.task_sem = self.SEM  # type: asyncio.Semaphore|None

	async def main(self):
		pass

	async def _main_outer(self):
		try:
			async with self._stack:
				if self.task_sem:
					await self._stack.enter_async_context(self.task_sem)
				await self.main()
		except CancelledError:
			self.warning(f"Cancelled before completion.")
		except Shutdown:
			self.warning(f"Shutdown before completion.")
		except Exception as exc:
			self.error_exc(f"Failed", exc, exc_info=not isinstance(exc, BaseException))
			raise exc

	def start(self) -> 'asyncio.Task':
		self._task = asyncio.create_task(self._main_outer(), name=self.name)
		return self._task

	def wait(self) -> 'asyncio.Task':
		if not self._task:
			self.start()
		return self._task


class Base(Helper):
	def __init__(self):
		super().__init__(self)
		self.name = 'main'
		self.debug = False
		self.config = dict()

		self.shutdown_requested = asyncio.Event()
		self.io_sem = asyncio.Semaphore(8)

		self.db_engine = None  # type: asa.AsyncEngine|None
		self.db_meta = sa.MetaData()
		self.db_sem = asyncio.Semaphore(8)

	# # #

	def _config_load(self):
		import yaml
		with open('config.yml', mode='r', encoding='utf-8') as cf:
			config = yaml.safe_load(cf)
			if not isinstance(config, dict):
				raise TypeError(f"config: {type(config)}")
			self.config = config
			self.debug = bool(config['debug'])
			asyncio.get_event_loop().set_debug(self.debug)

	# # #

	def _logging_iter_children(self, logger: 'logging.Logger'):
		yield logger
		for logger in logger.getChildren():
			yield from self._logging_iter_children(logger)

	def _logging_init(self):
		level = logging.DEBUG if self.debug else logging.INFO

		std_formatter_type = ColorFormatter if sys.stderr.isatty() else logging.Formatter
		std_formatter = std_formatter_type('[%(asctime)s][%(levelname)s] %(message)s', datefmt='%m-%d %H:%M:%S')
		std_handler = logging.StreamHandler(stream=sys.stderr if sys.stderr.isatty() else sys.stdout)
		std_handler.setLevel(level)
		std_handler.setFormatter(std_formatter)

		file_base = self.config['log_file']
		pathlib.Path(file_base).absolute().parent.mkdir(parents=True, exist_ok=True)

		file_formatter = logging.Formatter('[%(asctime)s][%(name)s][%(levelname)s] %(message)s')
		file_handler = logging.handlers.RotatingFileHandler(
			file_base, maxBytes=10 * 1024 * 1024, backupCount=10, encoding='utf-8'
		)

		file_handler.setLevel(level)
		file_handler.setFormatter(file_formatter)

		logging.basicConfig(level=level)  # defaults

		for logger in list(self._logging_iter_children(logging.root)):
			for old_handler in logger.handlers:
				logger.removeHandler(old_handler)

		logging.root.setLevel(level=level)
		logging.root.addHandler(std_handler)
		logging.root.addHandler(file_handler)

		self.log.setLevel(level=level)
		self.info(f"Logger initialized! DEBUG={self.debug}")

	# # #

	def _async_prepare(self) -> 'asyncio.AbstractEventLoop':
		db_protocol, _ = str(self.config['db_url']).split('://', 1)
		db_protocol = db_protocol.lower()
		if 'psycopg' in db_protocol:
			asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

		loop = asyncio.new_event_loop()
		asyncio.set_event_loop(loop)
		self._signals_bind(loop)
		return loop

	# loop = asyncio.get_event_loop()
	# self.bind_signals(loop)

	async def _async_post(self):
		loop = asyncio.get_event_loop()
		loop.set_debug(self.debug)
		self.check_shutdown()

	# # #

	def shutdown(self, reason):
		self.warning(f"Shutdown requested: {reason!r}")
		self.shutdown_requested.set()

	async def _signals_wakeup(self):
		# signals might not trigger if event loop doing nothing and chilling in await state,
		# workaround to manually trigger loop activity once per second
		while True:
			await asyncio.sleep(1)

	def _signals_bind(self, loop: 'asyncio.AbstractEventLoop'):
		# must be called after db init because of bug in asyncpg
		try:
			loop.add_signal_handler(signal.SIGINT, lambda _: self.shutdown("loop SIGINT"), None)
			loop.add_signal_handler(signal.SIGTERM, lambda _: self.shutdown("loop SIGTERM"), None)
			self.info(f"Added loop signal handlers!")
			return
		except RuntimeError as exc:
			self.warning_exc(f"Can't {loop!r}.add_signal_handler(...)", exc)

		# fall-back handlers, add_signal_handler above usually not supported on Windows
		try:
			# some other things might be dependent on signals,
			# so we just pass signals received by our handler to previous one
			def _handle(signum, frame, handler=None):
				sigs = signal.strsignal(signum)
				self.shutdown(f"signal {signum} {sigs!r}")
				if callable(handler):
					self.warning(f"Passing signal {signum} {sigs!r} to {handler}...")
					handler(signum, frame)

			for signal_num in (signal.SIGINT, signal.SIGTERM, signal.SIGBREAK):
				old_handler = signal.getsignal(signal_num)
				# self.info(f"{signal_num=} {old_handler=}")
				signal.signal(signal.SIGINT, functools.partial(_handle, handler=old_handler))

			loop.create_task(self._signals_wakeup(), name='_signals_wakeup')  # workaround

			self.warning(f"Added fallback signal.signal handlers.")
		except RuntimeError as exc:
			self.warning(f"Can't signal.signal: {exc}", exc_info=exc)

	async def sleep_or_shutdown(self, sleep: 'int|float') -> 'bool':
		# asyncio.sleep() but shutdown_requested aware
		# returns true if shutdown was requested, else false
		if self.shutdown_requested.is_set():
			return False
		try:
			await asyncio.wait_for(self.shutdown_requested.wait(), sleep)
		except asyncio.TimeoutError:
			pass
		return not self.shutdown_requested.is_set()

	# # #

	async def _db_version(self):
		self.check_shutdown()
		try:
			begin = time.monotonic()
			async with self.db_engine.connect() as conn:
				self.check_shutdown()
				statement = sa.text('SELECT version()')
				result = await conn.execute(statement)  # type: sa.engine.Result
				db_version = result.scalar_one()
			span = datetime.timedelta(seconds=time.monotonic() - begin)
			self.info(f"Database version: {db_version!r}, took {span}.")
		except Exception as exc:
			self.error_exc(f"Failed to connect database", exc)
			raise exc
		self.check_shutdown()

	async def _db_load_meta(self):
		self.check_shutdown()
		try:
			begin = time.monotonic()
			async with self.db_engine.connect() as conn:
				self.check_shutdown()
				await conn.run_sync(lambda conn_s: self.db_meta.reflect(bind=conn_s, views=True))
			span = datetime.timedelta(seconds=time.monotonic() - begin)
			self.info(f'Reflected {len(self.db_meta.tables)} database tables in {span}.')
		except Exception as exc:
			self.error_exc(f"Failed to reflect database", exc)
			raise exc
		self.check_shutdown()

	async def _db_prepare(self):
		db_url = self.config['db_url']
		self.info(f"Connecting to database {db_url!r}...")
		self.db_engine = asa.create_async_engine(db_url, echo=self.debug, echo_pool=self.debug,
			pool_size=5, max_overflow=10, pool_pre_ping=True, pool_timeout=300, pool_recycle=1200)
		await self._db_version()
		await self._db_load_meta()
		self.info(f'Database is ready.')

	# # #

	async def sub_main(self):
		# await self.shutdown_requested.wait()
		while True:
			await asyncio.sleep(1)
			if self.shutdown_requested.is_set():
				break

	async def cleanup_loop(self) -> None:
		while True:
			current = asyncio.current_task()
			tasks = list(t for t in asyncio.all_tasks() if t != current and not t.done())
			if not tasks:
				return
			names = list(task.get_name() or str(task) for task in tasks)
			self.info(f"Cancelling all {len(tasks)} tasks: {names!r}")
			for task in tasks:
				task.cancel()
			done, pending = await asyncio.wait(tasks, timeout=5)
			self.info(f"Cancelled {len(done)} tasks, {len(pending)} not yet done.")

	async def async_main(self):
		try:
			await self._async_post()
			await self._db_prepare()
			self.info("Initialized, running sub_main...")
			await self.sub_main()
			self.info("Done sub_main.")
		except CancelledError:
			self.warning(f"Cancelled before completion.")
		except Shutdown:
			self.warning(f"Shutdown before completion.")
		except Exception as exc:
			self.error_exc(f"sub_main failed", exc, exc_info=not isinstance(exc, BaseException))
			raise exc
		finally:
			await self.cleanup_loop()

	def main(self):
		self._config_load()
		self._logging_init()

		loop = self._async_prepare()
		try:
			main_task = loop.create_task(self.async_main())
			loop.run_until_complete(main_task)
		finally:
			loop.close()

		self.info("Done main asyncio loop.")


if __name__ == "__main__":
	Base().main()
