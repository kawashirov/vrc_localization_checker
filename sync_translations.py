import asyncio
import contextlib
import os
import pathlib
import re
import time

import aiofiles
import aiofiles.os

import sqlalchemy as sa
import sqlalchemy.ext.asyncio as asa
from sqlalchemy.dialects import postgresql as sa_pgsql

import base


class SyncLangTask(base.Task):
	SEM = asyncio.Semaphore(8)

	def __init__(self, base_, folder_task: 'SyncFolderTask', lang_file: 'pathlib.Path'):
		super().__init__(base_)
		self.folder_task = folder_task  # type: SyncFolderTask
		self.lang_file = lang_file  # type: pathlib.Path # absolute
		self.lang_code = lang_file.stem
		self.l10n_name = folder_task.path_parent.name
		self.name = f"{folder_task.name}/{self.lang_code}"
		self.lines = list()

	async def read_lines(self):
		self.check_shutdown()
		self.info(f"Reading lines from \"{self.lang_file}\"...")
		self.lines.clear()
		begin = time.monotonic()
		async with aiofiles.open(self.lang_file, mode='r', encoding='utf-8') as fp:
			async for line in fp:
				self.lines.append(str(line).strip())
				self.check_shutdown()
		if len(self.lines) != len(self.folder_task.keys):
			self.base.error_raise(RuntimeError, f"Number of lines ({len(self.lines)}) doesn't match with number of keys ({len(self.folder_task.keys)})!")
		self.info(f"Found {len(self.lines)} lines in {self.span(begin)}...")

	async def store_lines(self):
		self.check_shutdown()
		self.info(f"Storing {len(self.lines)} to DB...")
		t = self.base.db_t_translations()
		statement = sa_pgsql.insert(t).on_conflict_do_nothing()
		begin = time.monotonic()
		async with contextlib.AsyncExitStack() as stack:
			conn = await self.base.db_conn(stack)
			self.check_shutdown()
			for i, (key, body) in enumerate(zip(self.folder_task.keys, self.lines)):
				# todo try
				row = dict(string_file=self.l10n_name, string_key=str(key), lang_code=str(self.lang_code), string_body=str(body))
				await conn.execute(statement, row)
				self.check_shutdown() # todo commit part
			await conn.commit()
		self.info(f"Stored {len(self.lines)} to DB in {self.span(begin)}.")

	async def main(self):
		await self.read_lines()
		await self.store_lines()


class SyncFolderTask(base.Task):
	LANG_PATTERN = re.compile(r'^[a-zA-Z_-]+\.txt$')

	def __init__(self, base_, key: 'pathlib.Path'):
		super().__init__(base_)
		self.path_key = key  # type: pathlib.Path # absolute
		self.path_parent = self.path_key.parent  # type: pathlib.Path
		self.name = self.path_parent.name
		self.keys = list()  # type: list[str]
		self.langs = list()  # type: list[SyncLangTask]

	async def read_keys(self):
		self.check_shutdown()
		self.info(f"Reading keys...")
		self.keys.clear()
		begin = time.monotonic()
		async with aiofiles.open(self.path_key, mode='r', encoding='utf-8') as fp:
			async for line in fp:
				self.keys.append(str(line).strip())
				self.check_shutdown()
		self.info(f"Found {len(self.keys)} keys in {self.span(begin)}...")

	async def find_languages(self):
		# async find all lang.txt files
		self.check_shutdown()
		self.info(f"Looking for languages...")
		begin = time.monotonic()
		entries = await aiofiles.os.scandir(self.path_parent)
		self.check_shutdown()
		for entry in entries:  # type: os.DirEntry
			if entry.name.lower() == 'keys.txt':
				continue
			if not self.LANG_PATTERN.fullmatch(entry.name):
				continue
			if not await aiofiles.os.path.isfile(entry.path):
				continue
			self.check_shutdown()
			lang_file = pathlib.Path(entry.path)
			sub_task = SyncLangTask(self.base, self, lang_file)
			self.langs.append(sub_task)
			sub_task.start()
		self.info(f"Found {len(self.langs)} languages in {self.span(begin)}...")

	async def main(self):
		self.info(f"Syncing from \"{self.path_parent}\"...")
		await self.read_keys()
		await self.find_languages()
		self.info(f"Running {len(self.keys)} lang sync tasks...")
		begin = time.monotonic()
		self.check_shutdown()
		await self.gather(self.langs)
		self.info(f"Done {len(self.keys)} lang sync tasks in {self.span(begin)}.")


class SyncTranslations(base.Base):
	def __init__(self):
		super().__init__()
		self.name = 'sync_t9ns'
		self.keys = set()  # type: set[pathlib.Path]

	def find_l10n_root(self):
		l10n_root = self.config.get('localization_folder')

		if not l10n_root:  # default
			env_appdata = os.getenv('LOCALAPPDATA', '')
			if not env_appdata:
				raise RuntimeError("No %LOCALAPPDATA% is set?")
			vrc_root = pathlib.Path(env_appdata).parent / 'LocalLow' / 'VRChat' / 'VRChat'
			l10n_root = vrc_root / 'Localization'

		return pathlib.Path(l10n_root).absolute()

	async def find_keys(self):
		l10n_root = self.find_l10n_root()
		self.info(f"Looking for key files in \"{l10n_root}\"...")
		for root, _, files in os.walk(l10n_root):
			for file in files:
				if str(file).lower() != "keys.txt": continue
				path = pathlib.Path(root, file).absolute()
				self.keys.add(path)
				self.info(f"Found key file: \"{path}\"...")
				await asyncio.sleep(0)  # no long locks
			self.check_shutdown()
		self.info(f"Found {len(self.keys)} key files.")
		self.check_shutdown()

	async def refresh_views(self):
		self.check_shutdown()  # todo should actually leave?
		self.info(f"Refreshing DB materialized views...")
		t_lt = self.base.db_t_latest_translations()
		view_name = sa.quoted_name(t_lt.name, quote=True)
		statement = sa.text(f"REFRESH MATERIALIZED VIEW {view_name}")
		begin = time.monotonic()
		async with contextlib.AsyncExitStack() as stack:
			conn = await self.base.db_conn(stack)
			await conn.execute(statement)
			await conn.commit()
		self.info(f"Refreshed DB materialized views in {self.span(begin)}.")

	async def sub_main(self):
		await self.find_keys()

		key_tasks = [SyncFolderTask(self, key) for key in self.keys]
		self.info(f"Running {len(self.keys)} key sync tasks...")
		await self.gather(key_tasks)
		self.info(f"Done {len(self.keys)} key sync tasks.")

		await self.refresh_views()

		# await self.shutdown_requested.wait()
		pass


if __name__ == "__main__":
	SyncTranslations().main()
