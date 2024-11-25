import os
import re
from fnmatch import translate as fntranslate
from urllib.parse import urlparse
from stat import S_ISDIR
from shutil import copy2, rmtree
from .utils import to_list
from .ssh import SSH


file_match = lambda file, patterns: re.compile(
    "|".join(fntranslate(pattern) for pattern in patterns)
).match(os.path.basename(file))


def sync(source, target, subfolders=True, pattern="*.*", exclude=None, parse_target=None):
    folder_sync = FolderSync(source, target, subfolders, pattern, exclude, parse_target)
    result = folder_sync.sync(parse_target=parse_target)
    folder_sync.close()
    return result


class FolderSync:
    def __init__(
        self,
        source,
        target,
        subfolders=True,
        pattern="*.*",
        exclude=None,
        parse_target=None,
    ):
        self.patterns = to_list(pattern or "")
        self.exclude = to_list(exclude or "")
        self.source = (
            Folder(source, subfolders, self.patterns, self.exclude)
            if isinstance(source, str)
            else source
        )
        self.target = (
            Folder(target, subfolders, self.patterns, self.exclude)
            if isinstance(target, str)
            else target
        )
        self.parse_target = parse_target

    def sync_file(self, file, parse_target=None):
        source_file = os.path.join(self.source.folder, file)
        parse_target = parse_target(source_file) if parse_target or self.parse_target else ""
        target_file = os.path.join(self.target.folder, parse_target, file)
        self.target.makedir(os.path.dirname(target_file))
        if self.source.sftp and self.target.sftp:
            with (self.source.sftp.file if self.source.sftp else open)(source_file, "rb") as fl:
                fl.prefetch()
                self.target.sftp.putfo(fl, target_file)
        elif self.source.sftp:
            self.source.sftp.get(source_file, target_file)
        elif self.target.sftp:
            self.target.sftp.put(source_file, target_file)
        else:
            copy2(source_file, target_file)
        if self.source.sftp or self.target.sftp:
            stat = self.source.sftp.stat(source_file) if self.source.sftp else os.stat(source_file)
            (self.target.sftp.utime if self.target.sftp else os.utime)(target_file, (stat.st_atime, stat.st_mtime))
        return target_file, self.target


    def diff_list(self):
        source = self.source.scandir()
        target = self.target.scandir()
        return [file for file in source if os.path.basename(file)
            not in (os.path.basename(f) for f in target)]

    def sync_generator(self, diff_only=True, parse_target=None):
        for file in self.diff_list() if diff_only else self.source.scandir():
            yield self.sync_file(file, parse_target)

    def sync(self, diff_only=True, parse_target=None):
        result = []
        for file, folder in self.sync_generator(diff_only, parse_target):
            result.append((file, folder))
        return result

    def close(self):
        if self.source:
            self.source.close()
        if self.target:
            self.target.close()

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.close()


class Folder:
    def __init__(self, url=".", subfolders=True, pattern="*.*", exclude=None):
        url_parse = urlparse(url)
        self.folder = os.path.expandvars(os.path.expanduser(url_parse.path))
        self.subfolders = subfolders
        self.pattern = to_list(pattern or "")
        self.exclude = to_list(exclude or "")
        self.ssh = None
        self.sftp = None
        if url_parse.scheme.lower() in ("ssh", "ftp", "sftp", "scp"):
            self.ssh = SSH(url).ssh
            self.sftp = self.ssh.open_sftp()
        # self.makedir(self.folder)

    def scandir(self, subfolder="", full_path=False, pattern=None, exclude=None):
        result = []
        pattern = to_list((pattern or self.pattern) or "")
        exclude = to_list((exclude or self.exclude) or "")
        folder = os.path.join(self.folder, subfolder)
        scandir_func = self.sftp.listdir_attr if self.sftp else os.scandir
        for file in scandir_func(folder):
            st_mode = file.st_mode if self.sftp else file.stat().st_mode
            filename = os.path.join(
                folder if full_path else subfolder,
                file.filename if self.sftp else file.name,
            )
            if self.subfolders and S_ISDIR(st_mode):
                result.extend(
                    self.scandir(filename, pattern=pattern, exclude=exclude)
                )
            elif file_match(filename, pattern) and not file_match(
                filename, exclude
            ):
                result.append(filename)
        return result

    def makedir(self, dir):
        if self.sftp:
            try:
                self.sftp.stat(dir)
            except IOError:
                self.sftp.mkdir(dir)
        else:
            os.makedirs(dir, exist_ok=True)

    def purge(self, subfolders=None, pattern=None, exclude=None):
        subfolders = subfolders or self.subfolders
        pattern = to_list((pattern or self.pattern) or "*.*")
        exclude = to_list((exclude or self.exclude) or "")
        for root, dirs, files in os.walk(self.folder):
            for file in filter(
                lambda f: file_match(f, pattern) and not file_match(f, exclude), files
            ):
                os.unlink(os.path.join(root, file))
            if subfolders:
                for dir in dirs:
                    rmtree(os.path.join(root, dir))

    def __enter__(self):
        return self

    def __str__(self):
        return self.folder

    def __repr__(self):
        return f"Folder {self.folder}"

    def close(self):
        if self.ssh:
            if self.sftp:
                self.sftp.close()
            self.ssh.close()

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.close()
