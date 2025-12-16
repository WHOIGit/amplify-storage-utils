import os
import hashlib
import base64
import re

from storage.object import ObjectStore
from storage.utils import KeyTransformingStore, KeyTransformer


class FilesystemStore(ObjectStore):
    def __init__(self, root_path):
        self.root_path = root_path

    def _path(self, key):
        return os.path.join(self.root_path, key)

    def put(self, key, data):
        os.makedirs(os.path.dirname(self._path(key)), exist_ok=True)
        with open(self._path(key), 'wb') as f:
            f.write(data)

    def get(self, key):
        try:
            with open(self._path(key), 'rb') as f:
                return f.read()
        except FileNotFoundError:
            raise KeyError(key)
        
    def exists(self, key):
        return os.path.exists(self._path(key))
    
    def delete(self, key):
        try:
            os.remove(self._path(key))
            return True
        except FileNotFoundError:
            raise KeyError(key)

    def keys(self):
        for dirpath, dirnames, filenames in os.walk(self.root_path):
            for filename in filenames:
                yield os.path.relpath(os.path.join(dirpath, filename), self.root_path)


def hashpath(key, width=2, depth=3):
    hash = hashlib.sha256(key.encode('utf-8')).hexdigest()
    # split hash into chunks of size 'width' up to the specified depth
    path_components = [hash[i:i+width] for i in range(0, width*depth, width)]
    # append the remaining part of the hash as a single component
    path_components.append(hash[width*depth:])
    return os.path.join(*path_components)


class HashdirStore(KeyTransformingStore):
    """A store that transforms keys into hash-based paths before storing them in a filesystem.
    
    This helps distribute files across directories to avoid having too many files
    in a single directory, which can cause performance issues in some filesystems.
    """
    
    def __init__(self, root_path, width=2, depth=3):
        """Initialize the store.
        
        Args:
            root_path: Base directory for storing files
            width: Width of each directory level in characters
            depth: Number of directory levels to create
        """
        self.width = width
        self.depth = depth
        # Create the backing FilesystemStore
        fs_store = FilesystemStore(root_path)
        super().__init__(fs_store)
    
    def transform_key(self, key):
        """Transform a key into a hash-based path."""
        return hashpath(key, self.width, self.depth)

    def reverse_transform_key(self, key):
        """HashdirStore cannot reliably reverse the hash transformation,
        so listing keys is not supported."""
        raise NotImplementedError

    def keys(self):
        """HashdirStore does not support listing keys since the transformation
        cannot be reversed."""
        raise NotImplementedError


class FilesystemKeyTransformer(KeyTransformer):
    # Windows reserved filenames (case-insensitive, no extension)
    WINDOWS_RESERVED = {
        "CON", "PRN", "AUX", "NUL",
        *(f"COM{i}" for i in range(1, 10)),
        *(f"LPT{i}" for i in range(1, 10)),
    }

    def transform_key(self, key: str) -> str:
        """
        Transform an arbitrary Unicode key into a reversible, filesystem-safe filename.
        """
        # Encode key → UTF-8 → Base32
        b = key.encode("utf-8")
        encoded = base64.b32encode(b).decode("ascii")

        # Replace '=' padding with underscores
        encoded = encoded.rstrip("=")
        encoded = encoded + "_" * ((8 - len(encoded) % 8) % 8)

        # Avoid leading dot on UNIX-like systems
        if encoded.startswith("."):
            encoded = "_" + encoded

        # Avoid empty filename (should never happen, but for safety)
        if not encoded:
            encoded = "_"

        # Avoid Windows trailing dot/space
        encoded = encoded.rstrip(" .")
        if not encoded:
            encoded = "_"

        # Avoid Windows reserved filenames
        bare = encoded.upper().rstrip("_")
        if bare in self.WINDOWS_RESERVED:
            encoded = "_" + encoded

        return encoded

    def reverse_transform_key(self, filename: str) -> str:
        """
        Reverse-transform a filename produced by transform_key() back to the original Unicode key.
        """
        # Undo artificial prefix to avoid Windows reserved names
        if filename.startswith("_"):
            possible_original = filename[1:]
            bare = possible_original.upper().rstrip("_")
            if bare in self.WINDOWS_RESERVED:
                filename = possible_original

        # Convert trailing underscores → '=' (Base32 padding)
        match = re.match(r"(.*?)(_*)$", filename)
        stripped, underscores = match.groups()
        pad_count = len(underscores)
        encoded = stripped + ("=" * pad_count)

        # Decode Base32 → UTF-8 → Unicode
        decoded_bytes = base64.b32decode(encoded)
        return decoded_bytes.decode("utf-8")


# a filesystem store that uses FilesystemKeyTransformer
class SafeFilesystemStore(KeyTransformingStore):
    def __init__(self, root_path):
        fs_store = FilesystemStore(root_path)
        transformer = FilesystemKeyTransformer()
        super().__init__(fs_store, transformer)