import asyncio
import contextlib
import pathlib
import time
import pprint

import gspread
from oauth2client.service_account import ServiceAccountCredentials

import sqlalchemy as sa

import base


class ExportToGoogleSheet(base.Base):
	def __init__(self):
		super().__init__()
		self.name = 'sync_t9ns'
		self.keys = set()  # type: set[pathlib.Path]

		self.gspread_client = None  # type: gspread.Client|None
		self.spreadsheet = None  # type: gspread.Spreadsheet|None
		self.worksheet = None  # type: gspread.Worksheet|None
		self.rows = list()  # type: list[tuple[...]]

	def _gspread_connect(self):  # sync
		# See:
		# https://console.cloud.google.com/iam-admin/serviceaccounts
		# https://console.developers.google.com/apis/api/sheets.googleapis.com/overview
		scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
		keyfile = self.config['google_keyfile']
		spreadsheet_id = self.config['google_spreadsheet_id']
		self.info(f"Connecting to Google spreadsheet {spreadsheet_id!r} using \"{keyfile}\"...")
		begin = time.monotonic()
		creds = ServiceAccountCredentials.from_json_keyfile_name(keyfile, scope)
		self.gspread_client = gspread.authorize(creds)
		self.spreadsheet = self.gspread_client.open_by_key(spreadsheet_id)
		self.info(f"spreadsheet: {self.spreadsheet} in {self.span(begin)}.")

	# @functools.lru_cache
	def _select_suggestions_stmt(self) -> 'sa.Select':
		model_id = self.config['google_export_model_id']

		suggestions = self.base.db_t_suggestions()
		lt_source = sa.alias(self.base.db_t_translations(), name="lt_source")
		lt_target = sa.alias(self.base.db_t_translations(), name="lt_target")

		# noinspection PyTypeChecker
		query = (
			# main query: filtering out rows with already high number of suggestions_count and sorting the rest
			sa.select(
				lt_source.c.string_file,
				lt_source.c.string_key,
				lt_source.c.string_body.label("source_string_body"),
				lt_target.c.string_body.label("target_string_body"),
				suggestions.c.suggestion_string_body,
				suggestions.c.suggestion_comment,
				suggestions.c.added_at,
			)
			.select_from(suggestions)
			.join(lt_source, suggestions.c.source_id == lt_source.c.id)
			.join(lt_target, suggestions.c.target_id == lt_target.c.id)
			.where(
				suggestions.c.model_id == model_id,
				suggestions.c.suggestion_string_body != sa.null(),
			)
			.order_by(
				# lt_source.c.string_file.asc(),
				# lt_source.c.string_key.asc(),
				suggestions.c.added_at.asc(),
			)
		)
		return query

	async def _select_suggestions(self):
		self.check_shutdown()
		model_id = self.config['google_export_model_id']
		query = self._select_suggestions_stmt()
		self.info(f"Loading all the suggestions for {model_id!r}...")
		begin = time.monotonic()
		async with contextlib.AsyncExitStack() as stack:
			conn = await self.base.db_conn(stack)
			result = await conn.execute(query)
			self.rows = [tuple(str(v) for v in row.values()) for row in result.mappings()]
		self.info(f"Loaded {len(self.rows)} suggestions for {model_id!r} in {self.span(begin)}...")

	def _prepare_worksheet(self):
		model_id = self.config['openai_model']
		self.info(f"Preparing worksheet...")
		begin = time.monotonic()
		need_rows = len(self.rows) + 10
		need_cols = 10
		try:
			worksheets = self.spreadsheet.worksheets()
			self.worksheet = next(iter(w for w in worksheets if w.title == model_id))
			self.info(f"Got existing worksheet: {self.worksheet} in {self.span(begin)}.")
			min_rows = max(need_rows, self.worksheet.row_count)
			min_cols = max(need_cols, self.worksheet.col_count)
			if self.worksheet.row_count < min_rows or self.worksheet.col_count < min_cols:
				self.worksheet.resize(min_rows, min_cols)
				self.info(f"Resized {self.worksheet} to {(min_rows, min_cols)}.")
		except StopIteration:
			self.worksheet = self.spreadsheet.add_worksheet(model_id, len(self.rows) + 10, 10)
			self.info(f"Added new worksheet: {self.worksheet} in {self.span(begin)}.")

		# self.worksheet.columns_auto_resize(0, 10)
		pass

	def _update_worksheet(self):
		range_name = "B5"
		self.info(f"Updating {self.worksheet} at {range_name!r}...")
		begin = time.monotonic()
		self.worksheet.update(self.rows, 'B5')
		self.info(f"Updated {self.worksheet} in {self.span(begin)}...")

	async def sub_main(self):
		# todo fix blocking things

		self._gspread_connect()
		await asyncio.sleep(0)

		await self._select_suggestions()

		if self.debug:
			self.info(pprint.pformat(self.rows, width=240))
			await asyncio.sleep(0)

		self._prepare_worksheet()
		await asyncio.sleep(0)

		self._update_worksheet()
		await asyncio.sleep(0)

		pass


if __name__ == "__main__":
	ExportToGoogleSheet().main()
