import asyncio
import contextlib
import json
import pprint
import time
import functools
import typing
import datetime

import openai

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql as sa_pgsql

import base

if typing.TYPE_CHECKING:
	from openai.types.chat import ChatCompletion, ChatCompletionMessageParam
	from openai.types.shared_params import ResponseFormatJSONObject


class AnalyzeTranslations(base.Base):
	def __init__(self):
		super().__init__()
		self.name = 'analyze_t9ns'
		self.ai_client = None  # type: openai.AsyncOpenAI|None

	async def _init_openai(self):
		api_key = self.config.get('openai_api_key')
		base_url = self.config.get('openai_base_url')
		self.ai_client = openai.AsyncOpenAI(api_key=api_key, base_url=base_url)
		begin = time.monotonic()
		models = [model.id async for model in self.ai_client.models.list()]
		self.info(f"Got {len(models)} models from {base_url} in {self.span(begin)}.")
		if self.debug:
			self.info(pprint.pformat(models))

	@functools.lru_cache
	def _make_string_picker_stmt(self, limit: int):
		target_lang = self.config['target_lang']
		source_lang = self.config['source_lang']
		model_id = self.config['openai_model']
		max_suggestions = int(self.config.get('min_suggestions', 1))

		lt_target = sa.alias(self.base.db_t_latest_translations(), name="lt_target")
		lt_source = sa.alias(self.base.db_t_latest_translations(), name="lt_source")
		suggestions = self.base.db_t_latest_suggestions()

		pairs = (
			# all translation pairs: finds corresponding source_id and target_id
			# by string_file and string_key for given source_lang and target_lang
			sa.select(
				lt_target.c.string_file,
				lt_target.c.string_key,
				lt_source.c.id.label("source_id"),
				lt_target.c.id.label("target_id"),
				lt_source.c.string_body.label("source_string_body"),
				lt_target.c.string_body.label("target_string_body"),
				lt_target.c.added_at.label("target_added_at"),
				lt_source.c.added_at.label("source_added_at"),
			)
			.select_from(lt_target)
			.join(lt_source, sa.and_(
				lt_target.c.string_file == lt_source.c.string_file,
				lt_target.c.string_key == lt_source.c.string_key,
			))
			.where(sa.and_(
				lt_target.c.lang_code == target_lang,  # can be in join but postgresql are ok with this
				lt_source.c.lang_code == source_lang,
				lt_source.c.string_body != lt_target.c.string_body,  # important
			))
			.cte("pairs")
		)

		# noinspection PyTypeChecker
		sugg_counts = (
			# counts how many suggestions for each pair of source_id and target_id for given model_id
			# this only used in query for selecting pairs with lack of suggestions
			sa.select(
				suggestions.c.source_id,
				suggestions.c.target_id,
				sa.func.count(suggestions.c.added_at).label("cnt"),
			)
			.where(suggestions.c.model_id == model_id)  # typecheck error
			.group_by(suggestions.c.source_id, suggestions.c.target_id)
			.cte("sugg_counts")
		)

		joined = (
			# encapsulated into cte so suggestions_count can be used in where
			# merging in sugg_counts by source_id and target_id and with low number of sugg_counts
			sa.select(
				pairs.c.string_file,
				pairs.c.string_key,
				pairs.c.source_id,
				pairs.c.target_id,
				pairs.c.source_string_body,
				pairs.c.target_string_body,
				pairs.c.target_added_at,
				pairs.c.source_added_at,
				sa.func.coalesce(sugg_counts.c.cnt, 0).label("suggestions_count"),
			)
			.select_from(pairs)
			.outerjoin(sugg_counts, sa.and_(
				pairs.c.source_id == sugg_counts.c.source_id,
				pairs.c.target_id == sugg_counts.c.target_id,
			))
			.cte("joined")
		)

		query = (
			# main query: filtering out rows with already high number of suggestions_count and sorting the rest
			sa.select(joined)
			.select_from(joined)
			.where(joined.c.suggestions_count < max_suggestions)
			.order_by(
				joined.c.suggestions_count.asc(),
				joined.c.target_added_at.asc(),
				joined.c.source_added_at.asc(),
			)
			.limit(limit)
		)

		return query

	async def _pick_strings(self, limit: int) -> list[dict[str, ...]]:
		await self.db_refresh_views(self.db_t_latest_suggestions())
		self.check_shutdown()
		query = self._make_string_picker_stmt(limit)
		self.info(f"Picking strings with lack of suggestions...")
		begin = time.monotonic()
		async with contextlib.AsyncExitStack() as stack:
			conn = await self.base.db_conn(stack)
			result = await conn.execute(query)
			string_pairs = [dict(row) for row in result.mappings()]
		self.info(f"Picked {len(string_pairs)} strings with lack of suggestions in {self.span(begin)}.")
		return string_pairs

	@functools.lru_cache
	def _prepare_messages_prompt(self, source_lang: 'str', target_lang: 'str', initial_role="system") -> 'list[ChatCompletionMessageParam]':
		return [
			{"role": initial_role, "content": '\n'.join([
				"В следующих сообщениях тебе будет несколько раз представлены JSONы, в которые записаны некоторые фразы.",
				"JSON содержит object, со следующими полями:",
				"\"source_lang\" - string - это языковой код оригинального сообщения,",
				"\"source\" - string - это само оригинальное сообщение на исходном языке,",
				"\"target_lang\" - string - это языковой код целевого перевода оригинального сообщения на другой язык,",
				"\"target\" - string - это сам целевой перевод оригинального сообщения на другой язык,",
				"\"extra\" - object - это другие переводы оригинального сообщения на другие языки.",
				"",
				"Тебе НЕОБХОДИМО проверить, чтобы перевод фразы с \"source\" на \"target\" полностью удовлетворял всем указанным ниже требованиям.",
				"При этом для полноты понимания сути фраз можно опираться на другие переводы в \"extra\".",
				"Переводы в \"extra\" могут быть не точными, на них не следует опираться. \"source\" ВСЕГДА имеет приоритет.",
				"Предложи возможные исправления фразы в \"target\".",
				"",
				"Тематика: VRChat, интерфейс (UI), социальные сети и VR технологии.",
				"",
				"Требования и исключения из них:",
				"",
				"Фраза может быть чем угодно: как единственным словом, так и несколькими сложными предложениями.",
				"",
				"Язык фразы - строго русский, но могут встречаться и слова на английском. Это нормально.",
				"",
				"Предпочтительно в переводе не использовать англицизмы, сленги и прочие заимствования из других языков, если подходящее слово уже есть в русском языке.",
				"Однако, в силу тематики, часто подходящих слов в русском языке нет. В таких случаях заимствования допустимы.",
				"",
				"Основная форма обращения - на 'вы' (не с заглавной буквы), но есть и ряд явно шутливых и игривых фраз, где обращение на 'ты'.",
				"",
				"Во фразе могут быть шаблоны форматирования типа {0}, {{total}}, <color>, &nbsp; и т.п. Это нормально.",
				"НЕ НУЖНО ДУМАТЬ О ФОРМАТИРОВАНИИ И ШАБЛОНАХ, они остаются 'как есть'. НУЖНО думать о тексте, его качестве и содержимом.",
				"",
				"Могут встречаться сокращения 'эл. почта' и слова написанные КАПСОМ. Это тоже нормально.",
				"",
				"Могут использоваться записи вида 'одно/другое/третье' (через '/'). Это нормально.",
				"",
				"Могут встречаться аббревиатуры на русском или английском типа IK, VR или FBT. Это тоже нормально.",
				"",
				"В конце некоторых предложениях может не совпадать наличие точки в конце. Она может либо появиться, либо исчезнуть относительно оригинала. Это нормально.",
				"",
				"В оригинале часто используется Title/Camel Case, но мы предпочитаем в переводе использовать Sentence case, по этому",
				"слова, не являющиеся аббревиатурами, терминами и именами собственными, не должны неожиданно посреди предложения начинаться с заглавной буквы.",
				"",
				"Проверь буквы 'е' на возможность замены на 'ё'. Мы используем правило строгого 'ё',",
				"т.е. 'истёк' - ПРАВИЛЬНО, 'истек' - ОШИБКА, которую НЕОБХОДИМО исправить.",
				"",
				"Обрати внимание на общую грамматику, опечатки, пропущенные буквы, склонения, падежи, структуру предложения и т.п. характерную для Русского языка.",
				"",
				"Также нужно исправлять предложения вида 'Сделать что-то, чтобы ещё что-то' на 'Сделать что-то для чего-то ещё',",
				"т.е. заменять конструкцию 'чтобы' на запись через 'для', ради упрощения структуры предложения,",
				"но ТОЛЬКО если исправленное предложение будет звучать корректно и естественно.",
				"Также не стоит этого делать, если это вызовет повторение 'для' в исправленном предложении. Повторы звучат и читаются не очень хорошо.",
				"",
				"Несколько предложений можно соединять в одно, как и одно предложение разбивать на несколько, но лучше, если ",
				"предложений в результате окажется столько же, сколько и было изначально. По возможности сохраняй их количество.",
				"",
				"Подумай об этих правилах описанных выше. Подумай над КАЖДЫМ словом, словосочетанием, фразой и предложении, на соответствие этим требованиям.",
				"Фраза может НЕ содержать ошибок и соответствовать правилам описанным выше, если это так, то искусственно выдумывать в ней ошибки НЕ нужно.",
				"",
				"Твой ответ ДОЛЖЕН представлять из себя ТОЛЬКО JSON object и ничего больше.",
				"В этом JSON object ОБЯЗАТЕЛЬНО ВСЕГДА должно быть поле \"has_suggestion\" содержащее ТОЛЬКО boolean значение:",
				"true если у тебя ЕСТЬ предложение по улучшению, или false если у тебя НЕТ предложений по улучшению.",
				"Если ты выбираешь \"has_suggestion\": true, то ты ОБЯЗАН добавить поля и \"suggestion\", и \"comment\".",
				"Если ты выбираешь \"has_suggestion\": false, то ты ОБЯЗАН НЕ добавлять поле \"suggestion\", но можешь добавить \"comment\".",
				"Объяснение полей:",
				"\t\"suggestion\" - string - предлагаемый новый исправленный вариант фразы на замену \"target_код\",",
				"\t\"comment\" - string - объяснение, что предлагается исправить и почему.",
				"Ни каких других полей JSON содержать НЕ ДОЛЖЕН, иначе это будет ОШИБКА.",
				"",
				"\"suggestion\" НЕ должен совпадать с \"target_lang\", если они совпадают, значит ты НЕ сделал изменений,",
				"а если ты не сделал изменений, значит \"has_suggestion\" ДОЛЖЕН БЫТЬ false, а значит \"suggestion\" ДОЛЖЕН отсутствовать.",
				"",
				"\"comment\" (если присутствует) должен отвечать на вопрос 'Что сделать?' и ПОДРОБНО объяснять,",
				"что именно ты предлагаешь исправить и как, какие буквы изменятся в каких словах.",
				"",
				"Твой ответ должен быть валидным JSONом. НИКАКИХ дополнительных тегов, кодов, форматирований (типа Markdown) или хитрых мета-синтаксисов,",
				"которых не было в \"target_код\", в твоём ответе НЕ ДОЛЖНО БЫТЬ.",
				"",
				"Осознай требования к твоим ответам выше, покорно и внимательно исполняй их и \"не тупи\".",
			])},
			# Few-shot Learning
			# https://api-docs.deepseek.com/guides/kv_cache#example-3-using-few-shot-learning
			# Example 1
			{"role": "user", "content": json.dumps(dict(
				source_lang=source_lang,
				source="The mirror will snap to specific angles when enabled.",
				target_lang=target_lang,
				target="Выравнивать зеркла по определенным углам",
			), ensure_ascii=False)},
			{"role": "assistant", "content": json.dumps(dict(
				has_suggestion=True,
				suggestion="Выравнивать зеркала по определённым углам",
				comment="Исправить пропущенную букву 'а' в слове 'зеркала' и исправить букву 'ё' в слове 'определённым'."
			), ensure_ascii=False)},
			# Example 2
			{"role": "user", "content": json.dumps(dict(
				source_lang=source_lang,
				source="Set Home World",
				target_lang=target_lang,
				target="Установить как домашний мир",
			), ensure_ascii=False)},
			{"role": "assistant", "content": json.dumps(dict(
				has_suggestion=False,
			), ensure_ascii=False)},
			# Example 3
			{"role": "user", "content": json.dumps(dict(
				source_lang=source_lang,
				source="Confirmation dialogs are disabled, click to enable",
				target_lang=target_lang,
				target="Запрос подтверждения выключен, нажмите, чтобы включить его",
			), ensure_ascii=False)},
			{"role": "assistant", "content": json.dumps(dict(
				has_suggestion=True,
				suggestion="Запрос подтверждения выключен, нажмите для его включения",
				comment="Изменить структуру предложения со 'чтобы' на 'для'."
			), ensure_ascii=False)}
		]

	def _pairs_log_token(self, string_pairs: 'dict'):
		string_file, string_key = string_pairs['string_file'], string_pairs['string_key']
		source_id, target_id = string_pairs['source_id'], string_pairs['target_id']
		log_token = f"translation №{source_id} -> №{target_id} on string (\"{string_file}\", \"{string_key}\")"
		return log_token

	def _pick_extra_stmt(self, string_pair: 'dict[str, ...]'):
		# todo caching?
		target_lang = self.config['target_lang']
		source_lang = self.config['source_lang']
		lt = self.base.db_t_latest_translations()
		extra = (
			sa.select(
				lt.c.id,
				lt.c.lang_code,
				lt.c.string_body,
			)
			.select_from(lt)
			.where(sa.and_(
				# picking up the strings
				# which match given file and key
				lt.c.string_file == string_pair['string_file'],
				lt.c.string_key == string_pair['string_key'],
				# but isn't languages we operate on
				lt.c.lang_code != target_lang,
				lt.c.lang_code != source_lang,
				# and have distinct body
				lt.c.string_body != string_pair['source_string_body'],
				lt.c.string_body != string_pair['target_string_body'],
			))
		)
		return extra

	async def _pick_extra(self, string_pair: 'dict[str, ...]') -> 'list[dict[str, ...]]':
		log_token = self._pairs_log_token(string_pair)
		self.check_shutdown()
		query = self._pick_extra_stmt(string_pair)
		self.info(f"Picking extra context strings for context for {log_token}...")
		begin = time.monotonic()
		async with contextlib.AsyncExitStack() as stack:
			conn = await self.base.db_conn(stack)
			result = await conn.execute(query)
			extra = [dict(row) for row in result.mappings()]
		self.info(f"Picked {len(extra)} extra context strings for {log_token} in {self.span(begin)}.")
		return extra

	async def _complete(self, string_pair: 'dict[str, ...]', extra: 'list[dict[str, ...]]') -> 'tuple[ChatCompletion,datetime.timedelta]':
		log_token = self._pairs_log_token(string_pair)
		self.info(f"Preparing to check {log_token}...")
		source_lang, target_lang = self.config['source_lang'], self.config['target_lang']
		model_id = self.config['openai_model']

		extra_ids = {e['id'] for e in extra}
		extra_clear = {e['lang_code']: e['string_body'] for e in extra}
		message_content = json.dumps(dict(
			source_lang=source_lang,
			source=string_pair['source_string_body'],
			target_lang=target_lang,
			target=string_pair['target_string_body'],
			# extra=extra_clear,
		), ensure_ascii=False)
		self.info(f"Asking {model_id!r}:\n{message_content!r}")
		messages = [
			*self._prepare_messages_prompt(source_lang, target_lang),
			{"role": "user", "content": message_content}
		]
		self.info(f"Asking {model_id!r} to check {log_token} with extra={extra_ids!r}...")
		begin = time.monotonic()
		response = await self.ai_client.chat.completions.create(
			model=model_id, messages=messages, stream=False,
			n=1,  # can only use n=1 for now to properly count tokens
			response_format=openai.types.shared_params.ResponseFormatJSONObject(type="json_object"),
		)
		interval = self.span(begin)
		self.info(f"{model_id!r} responded on {log_token} in {interval}.")
		# self.info(pprint.pformat(response))
		return response, interval

	async def _process_suggestion(self, string_pair: 'dict', response: 'ChatCompletion', interval: 'datetime.timedelta'):
		log_token = self._pairs_log_token(string_pair)
		model_id = self.config['openai_model']

		self.info(f"Processing ")
		suggestion_json = dict()
		choice = response.choices[0]  # can only use n=1 for now to properly count tokens

		try:
			suggestion_json = json.loads(choice.message.content)
		except Exception as exc:
			self.warning_exc(f"Failed to decode model {model_id!r} response on {log_token}: {choice=}", exc, exc_info=False)

		if not isinstance(suggestion_json, dict):
			self.warning(f"Model {model_id!r} responded with with non-dict object on {log_token}: {pprint.pformat(suggestion_json)}")
			suggestion_json = dict()

		has_suggestion = suggestion_json.get('has_suggestion')
		if not isinstance(has_suggestion, bool):
			self.warning(f"Model {model_id!r} didn't provided valid 'has_suggestion' on {log_token}: {pprint.pformat(suggestion_json)}")
			has_suggestion = False

		suggestion = suggestion_json.get('suggestion')
		if suggestion is not None and (not isinstance(suggestion, str) or len(suggestion) < 1):
			self.warning(f"Model {model_id!r} didn't provided valid 'suggestion' on {log_token}: {pprint.pformat(suggestion_json)}")
			suggestion = None
			has_suggestion = False

		if string_pair['target_string_body'] == suggestion:
			self.warning(f"Model {model_id!r} suggested exactly the same 'target' on {log_token}: {pprint.pformat(suggestion_json)}")
			suggestion = None
			has_suggestion = False

		comment = suggestion_json.get('comment')
		if comment is not None and (not isinstance(comment, str) or len(comment) < 1):
			self.warning(f"Model {model_id!r} didn't provided valid 'comment' on {log_token}: {pprint.pformat(suggestion_json)}")
			comment = None

		self.info(f"Storing suggestion ({has_suggestion!r}) for {log_token} to DB...")
		t = self.base.db_t_suggestions()
		statement = sa_pgsql.insert(t).on_conflict_do_nothing()
		begin = time.monotonic()
		async with contextlib.AsyncExitStack() as stack:
			conn = await self.base.db_conn(stack)
			self.check_shutdown()
			# todo try
			row = dict(
				source_id=string_pair['source_id'],
				target_id=string_pair['target_id'],
				model_id=model_id,
				# added_at default
				suggestion_string_body=suggestion,
				suggestion_comment=comment,
				interval=interval,
				completion_tokens=response.usage and response.usage.completion_tokens or None,
				prompt_tokens=response.usage and response.usage.prompt_tokens or None,
				system_fingerprint=response.system_fingerprint
			)
			await conn.execute(statement, row)
			await conn.commit()
		self.info(f"Stored suggestion ({has_suggestion!r}) for {log_token} to DB in {self.span(begin)}.")

		self.info(pprint.pformat(suggestion))

	async def sub_main(self):
		await self._init_openai()
		while not self.shutdown_requested.is_set():
			string_pairs = await self._pick_strings(10)
			if len(string_pairs) < 1:
				self.info(f"No more strings with lack of suggestions found, done?")
				break
			# self.info(pprint.pformat(string_pairs))
			for string_pair in string_pairs:
				if self.shutdown_requested.is_set():
					break
				extra = await self._pick_extra(string_pair)
				if self.shutdown_requested.is_set():
					break
				response, interval = await self._complete(string_pair, extra)
				await self._process_suggestion(string_pair, response, interval)
			pass
		pass


if __name__ == "__main__":
	AnalyzeTranslations().main()
