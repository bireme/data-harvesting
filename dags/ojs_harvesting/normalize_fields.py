from data_harvesting.dags.ojs_harvesting.common import is_article_url
from data_harvesting.dags.ojs_harvesting.common import is_doi
from data_harvesting.dags.ojs_harvesting.common import is_issn
import json
import re


def get_source_sas_id(db_conn, journal, year, volume, number):
  sql_volume = ""
  if (volume):
    sql_volume = " a.volume_serial = '%s' AND " % volume

  sql_number = ""
  if (number):
    sql_number = " a.issue_number = '%s' AND " % number

  sql = """
    SELECT
      b.id as code
    FROM
      biblioref_referencesource AS a,
      biblioref_reference AS b
    WHERE
      a.title_serial = %s AND
      a.reference_ptr_id = b.id AND """ + sql_volume + sql_number + """
      LEFT(b.publication_date_normalized, 4) = %s
    ORDER BY
      CASE
        WHEN b.status = -1 THEN -4
        ELSE b.status
      END DESC
    LIMIT 1
  """

  id = None
  with db_conn.cursor() as cursor:
    cursor.execute(sql, (journal, year))
    result = cursor.fetchone()
    if result:
      id = result[0]

  return id


def normalize_lang_code(code):
  def _normalize(c):
    if not isinstance(c, str):
      return c
    
    c_clean = c.strip().lower()
    if c_clean in ["por", "pt-br"]:
      return "pt"
    elif c_clean in ["eng", "en-us"]:
      return "en"
    elif c_clean in ["spa", "es-es"]:
      return "es"
    elif c_clean in ["fre", "fr-fr"]:
      return "fr"
    return c_clean

  if isinstance(code, list):
    return [_normalize(item) for item in code]
  elif isinstance(code, str):
    return _normalize(code)
  
  return code


def normalize_pagination(pages):
  pages_normalized = []

  if 'e-' in pages:
      pages = pages.replace('e-', '').strip()

      pages_json = {'_e': "e-" + str(pages)}
      pages_normalized.append(pages_json)
  elif pages.startswith('e'):
      pages = pages.replace('e', '').strip()

      pages_json = {'_e': "e-" + str(pages)}
      pages_normalized.append(pages_json)
  elif '-' in pages:
      pages = pages.split('-')
      pages_f = pages[0].strip()
      pages_l = pages[1].strip()

      pages_json = {'_f': str(pages_f), '_l': str(pages_l)}
      pages_normalized.append(pages_json)
  elif pages:
      pages_json = {'_f': pages, '_l': pages}

  return pages_normalized


def parse_identifiers(identifiers):
    record_info = {}

    for identifier in identifiers:
        if is_doi(identifier):
            record_info['doi'] = identifier
        """elif is_article_url(identifier.text):
            pass
        else:
            if 'identifier' in record_info:
                record_info['identifier'].append(identifier.text)
            else:
                record_info['identifier'] = [identifier.text]"""

    return record_info


def parse_relations(relations):
  for relation in relations:
      if is_article_url(relation):
          return json.dumps([{'_g': True, '_u': relation}])
  return None


def parse_sources(sources):
  record_info = {}

  for source in sources:
      if isinstance(source, str):
          text = source
          lang = None
      elif isinstance(source, dict):
          text = source.get('#text', '')
          lang = source.get('@xml:lang')
      else:
          continue

      if is_issn(text):
          continue
      elif is_doi(text):
          record_info['doi'] = text
      else:
          source_parts = text.split(';')

          if len(source_parts) == 3:
              record_info['pagination'] = source_parts.pop().strip()
          
          if len(source_parts) >= 1:
              record_info['journal'] = source_parts.pop(0).strip()

          if len(source_parts) >= 1:
              vol_num_year = source_parts.pop(0).strip()
          else:
              vol_num_year = ""
          pattern = r'(?:Vol\.|v\.)\s*(?P<volume>\d+)\s*(?:(?:No\.|N[úu]m\.|n\.)\s*(?P<number>\d+)\s*)?\((?P<year>\d{4})\)'
          match = re.match(pattern, vol_num_year)
          if match:
              record_info['volume'] = match.group('volume').strip()
              #record_info['year'] = match.group('year').strip()

              if match.group('number'):
                  record_info['number'] = match.group('number').strip()

  return record_info


def parse_multilang_field(raw_data):
    if not raw_data:
        return []
    if isinstance(raw_data, (str, dict)):
        raw_data = [raw_data]

    parsed_list = []
    for item in raw_data:
        if isinstance(item, str):
            text = item
            lang = None
        elif isinstance(item, dict):
            text = item.get('#text', '')
            lang = item.get('@xml:lang')
        else:
            continue

        if text:
            entry = {'text': text}
            if lang:
                entry['_i'] = normalize_lang_code(lang)
            parsed_list.append(entry)
    return parsed_list