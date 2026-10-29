"""Bounded passive HTTP evidence archive; usable without third-party packages."""
import hashlib
import http.client
import json
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path


def utc():
    return datetime.now(timezone.utc).isoformat()


def digest(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def save_json(path, data):
    path = Path(path)
    temporary = path.with_suffix(path.suffix + '.tmp')
    temporary.write_text(json.dumps(data, indent=2, sort_keys=True) + '\n')
    temporary.replace(path)


class Archive:
    def __init__(self, output, max_bytes=4_000_000_000, resume=False, delay=1.0):
        self.output = Path(output)
        if max_bytes < 1:
            raise ValueError('max_bytes must be positive')
        self.limit, self.delay = max_bytes, delay
        self.last = {}
        self.manifest_path = self.output / 'manifest.json'
        if self.manifest_path.exists():
            if not resume:
                raise ValueError('Snapshot already exists; use --resume or a fresh label')
            self.manifest = json.loads(self.manifest_path.read_text())
            self.verify()
        else:
            self.output.mkdir(parents=True, exist_ok=True)
            if any(self.output.iterdir()):
                raise ValueError('Nonempty output without manifest')
            self.manifest = {'schema_version': 1, 'created_at': utc(), 'entries': []}
        (self.output / 'raw').mkdir(exist_ok=True)
        self.used = sum(e.get('bytes', 0) for e in self.manifest['entries'])
        self.flush()

    def flush(self):
        self.manifest['max_bytes'] = self.limit
        save_json(self.manifest_path, self.manifest)

    def verify(self):
        for entry in self.manifest['entries']:
            if entry.get('path'):
                path = self.output / entry['path']
                if digest(path) != entry['sha256'] or path.stat().st_size != entry['bytes']:
                    raise ValueError('Evidence hash/size mismatch: ' + str(path))

    def fetch(self, url, label, retry_failed=True):
        old = [e for e in self.manifest['entries'] if e['url'] == url]
        if old and (not retry_failed or (old[-1].get('status') == 200 and old[-1].get('path'))):
            return old[-1]
        if urllib.parse.urlsplit(url).scheme not in ('http', 'https'):
            raise ValueError('Only public HTTP(S) source URLs are supported')
        record = {'url': url, 'label': label, 'retrieved_at': utc(), 'attempts': []}
        if self.used >= self.limit:
            record['error'] = 'budget_exhausted'
        else:
            host = urllib.parse.urlsplit(url).netloc
            for attempt in range(3):
                time.sleep(max(0, self.delay - (time.monotonic() - self.last.get(host, 0))))
                self.last[host] = time.monotonic()
                try:
                    request = urllib.request.Request(url, headers={'User-Agent': 'PublicRunResearchArchive/1.0'})
                    try:
                        response = urllib.request.urlopen(request, timeout=45)
                    except urllib.error.HTTPError as error:
                        response = error
                    with response:
                        record['status'] = response.code
                        record['final_url'] = response.url
                        record['headers'] = {k: v for k, v in response.headers.items()
                                             if k.lower() != 'set-cookie'}
                        record['attempts'].append({'status': response.code, 'at': utc()})
                        if response.code in (429, 500, 502, 503, 504) and attempt < 2:
                            retry_after = response.headers.get('Retry-After', '')
                            if retry_after:
                                try:
                                    wait = float(retry_after)
                                except ValueError:
                                    from email.utils import parsedate_to_datetime
                                    wait = (parsedate_to_datetime(retry_after) - datetime.now(timezone.utc)).total_seconds()
                                if wait > 60:
                                    record['error'] = 'deferred_retry_after'
                                    break
                                time.sleep(max(0, wait))
                            else:
                                time.sleep(2 ** attempt)
                            continue
                        name = hashlib.sha256(url.encode()).hexdigest() + '.body'
                        path = self.output / 'raw' / name
                        temp = path.with_suffix('.partial')
                        size = 0
                        try:
                            with temp.open('xb') as stream:
                                while True:
                                    chunk = response.read(min(1024 * 1024, self.limit - self.used - size + 1))
                                    if not chunk:
                                        break
                                    size += len(chunk)
                                    if self.used + size > self.limit:
                                        raise OverflowError('budget_exhausted')
                                    stream.write(chunk)
                            # A retried failed request never overwrites the previous body.
                            if path.exists():
                                path = path.with_name(path.stem + '-' + str(len(self.manifest['entries'])) + '.body')
                            temp.replace(path)
                        except BaseException:
                            temp.unlink(missing_ok=True)
                            raise
                        self.used += size
                        record.update(path=str(path.relative_to(self.output)), bytes=size, sha256=digest(path))
                        break
                except (urllib.error.URLError, http.client.HTTPException, TimeoutError, OSError, OverflowError, ValueError) as error:
                    record['error'] = str(error)
                    record['attempts'].append({'error': str(error), 'at': utc()})
                    if isinstance(error, OverflowError) or attempt == 2:
                        break
                    time.sleep(2 ** attempt)
            if record.get('path'):
                record.pop('error', None)
        self.manifest['entries'].append(record)
        self.flush()
        print(label, record.get('status', record.get('error')), record.get('bytes', 0), flush=True)
        return record

    def json(self, entry):
        if entry.get('status') != 200 or not entry.get('path'):
            return None
        try:
            return json.loads((self.output / entry['path']).read_text())
        except (ValueError, UnicodeError):
            return None
