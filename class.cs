using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace Dank
{
    public static class DankTable
    {
        private const string NaM = "/NaM/";
        private static readonly object fileLock = new object();
        private static readonly LruCache<string, CacheItem> _cache = new LruCache<string, CacheItem>(100);

        public static void CreateDatabase(string path, List<string> rows, string mainRow)
        {
            var settings = new TableSettings
            {
                KeyRow = mainRow,
                Separator = "|",
                DankVersion = "1.0"
            };

            foreach (var row in rows)
            {
                if (!Regex.IsMatch(row, @"^[a-zA-Z_][a-zA-Z0-9_]*$"))
                    throw new ArgumentException($"Row '{row}' is invalid. Row names must be in English.");
            }

            string settingsLine = $"KeyRow:{settings.KeyRow};Separator:{settings.Separator};DankVersion:{settings.DankVersion};";
            string headersLine = string.Join(settings.Separator, rows);

            using (var writer = new StreamWriter(path))
            {
                writer.WriteLine(settingsLine);
                writer.WriteLine(headersLine);
            }
        }

        public static void AddRow(string path, string row)
        {
            var settings = ReadSettings(path);
            var headers = ReadHeaders(path, settings.Separator);
            var parts = row.Split(new[] { settings.Separator }, StringSplitOptions.None);
            if (parts.Length != headers.Count)
                throw new ArgumentException("Row does not match headers count.");

            lock (fileLock)
            {
                File.AppendAllText(path, row + Environment.NewLine);
            }
        }

        public static void AddRows(string path, List<string> rows)
        {
            foreach (var row in rows)
            {
                AddRow(path, row);
            }
        }

        public static void RemoveRow(string path, string row)
        {
            var settings = ReadSettings(path);
            var tempPath = Path.GetTempFileName();
            using (var reader = new StreamReader(path))
            using (var writer = new StreamWriter(tempPath))
            {
                string line;
                while ((line = reader.ReadLine()) != null)
                {
                    if (line != row)
                    {
                        writer.WriteLine(line);
                    }
                }
            }

            File.Delete(path);
            File.Move(tempPath, path);
            _cache.Clear();
        }

        public static void RemoveRows(string path, List<string> rows)
        {
            foreach (var row in rows)
            {
                RemoveRow(path, row);
            }
        }

        public static void AddLine(string path, Dictionary<string, object> data)
        {
            var settings = ReadSettings(path);
            var headers = ReadHeaders(path, settings.Separator);

            if (settings.KeyRow != null && !data.ContainsKey(settings.KeyRow))
            {
                string newId = GenerateNextId(path, settings);
                data[settings.KeyRow] = newId;
            }

            List<string> encodedValues = new List<string>();
            foreach (var header in headers)
            {
                if (data.TryGetValue(header, out var value))
                {
                    string json = JsonSerializer.Serialize(value, value.GetType(), new JsonSerializerOptions { WriteIndented = false });
                    string encoded = Convert.ToBase64String(Encoding.UTF8.GetBytes(json));
                    encodedValues.Add(encoded);
                }
                else
                {
                    encodedValues.Add(NaM);
                }
            }

            string line = string.Join(settings.Separator, encodedValues);

            lock (fileLock)
            {
                File.AppendAllText(path, line + Environment.NewLine);
            }

            string id = data[settings.KeyRow].ToString();
            var cacheItem = new CacheItem
            {
                data = new Dictionary<string, JsonElement>(),
                last_modified = DateTime.Now
            };
            foreach (var kvp in data)
            {
                string json = JsonSerializer.Serialize(kvp.Value, kvp.Value.GetType(), new JsonSerializerOptions { WriteIndented = false });
                using var doc = JsonDocument.Parse(json);
                cacheItem.data[kvp.Key] = doc.RootElement.Clone();
            }
            _cache.AddOrUpdate($"{path}#{id}", cacheItem);
        }

        public static void RemoveLine(string path, object id)
        {
            string idStr = id.ToString();
            var settings = ReadSettings(path);
            var tempPath = Path.GetTempFileName();
            using (var reader = new StreamReader(path))
            using (var writer = new StreamWriter(tempPath))
            {
                writer.WriteLine(reader.ReadLine()); // Settings line
                writer.WriteLine(reader.ReadLine()); // Headers line

                string line;
                while ((line = reader.ReadLine()) != null)
                {
                    var parts = line.Split(settings.Separator);
                    string encodedId = parts[0];
                    string decodedId = DecodeBase64(encodedId);
                    if (decodedId != idStr)
                    {
                        writer.WriteLine(line);
                    }
                }
            }

            File.Delete(path);
            File.Move(tempPath, path);
            _cache.Invalidate($"{path}#{idStr}");
        }

        public static void EditData(string path, object line, string row, object data)
        {
            string idStr = line.ToString();
            var settings = ReadSettings(path);
            var headers = ReadHeaders(path, settings.Separator);
            int rowIndex = headers.IndexOf(row);
            if (rowIndex == -1)
                throw new ArgumentException($"Row '{row}' not found.");

            string json = JsonSerializer.Serialize(data, data.GetType(), new JsonSerializerOptions { WriteIndented = false });
            string encoded = Convert.ToBase64String(Encoding.UTF8.GetBytes(json));

            var tempPath = Path.GetTempFileName();
            using (var reader = new StreamReader(path))
            using (var writer = new StreamWriter(tempPath))
            {
                writer.WriteLine(reader.ReadLine()); // Settings line
                writer.WriteLine(reader.ReadLine()); // Headers line

                string fileLine;
                while ((fileLine = reader.ReadLine()) != null)
                {
                    var parts = fileLine.Split(settings.Separator);
                    string encodedId = parts[0];
                    string decodedId = DecodeBase64(encodedId);
                    if (decodedId == idStr)
                    {
                        parts[rowIndex] = encoded;
                        fileLine = string.Join(settings.Separator, parts);
                    }
                    writer.WriteLine(fileLine);
                }
            }

            File.Delete(path);
            File.Move(tempPath, path);

            var cacheKey = $"{path}#{idStr}";
            if (_cache.TryGet(cacheKey, out var cachedItem))
            {
                using var doc = JsonDocument.Parse(json);
                cachedItem.data[row] = doc.RootElement.Clone();
                cachedItem.last_modified = DateTime.Now;
                _cache.AddOrUpdate(cacheKey, cachedItem);
            }
        }

        public static T GetData<T>(string path, object line, string row)
        {
            string idStr = line.ToString();
            var cacheKey = $"{path}#{idStr}";

            if (_cache.TryGet(cacheKey, out var cachedItem))
            {
                if (cachedItem.data.TryGetValue(row, out var element))
                {
                    if (IsNaM(element))
                        return default;
                    return element.Deserialize<T>(new JsonSerializerOptions { WriteIndented = false });
                }
            }

            var settings = ReadSettings(path);
            var headers = ReadHeaders(path, settings.Separator);
            var data = new Dictionary<string, JsonElement>();
            bool found = false;

            using (var reader = new StreamReader(path))
            {
                reader.ReadLine(); // Skip settings
                reader.ReadLine(); // Skip headers

                string fileLine;
                while ((fileLine = reader.ReadLine()) != null)
                {
                    var parts = fileLine.Split(settings.Separator);
                    string encodedId = parts[0];
                    string decodedId = DecodeBase64(encodedId);
                    if (decodedId == idStr)
                    {
                        found = true;
                        for (int i = 0; i < headers.Count; i++)
                        {
                            string header = headers[i];
                            string encodedValue = parts[i];
                            string decodedValue = DecodeBase64(encodedValue);
                            if (decodedValue == NaM)
                            {
                                data[header] = default;
                                continue;
                            }

                            try
                            {
                                using var doc = JsonDocument.Parse(decodedValue);
                                data[header] = doc.RootElement.Clone();
                            }
                            catch
                            {
                                data[header] = default;
                            }
                        }
                        break;
                    }
                }
            }

            if (!found)
                return default;

            var cacheItem = new CacheItem
            {
                data = new Dictionary<string, JsonElement>(data),
                last_modified = DateTime.Now
            };
            _cache.AddOrUpdate(cacheKey, cacheItem);

            if (data.TryGetValue(row, out var elem))
            {
                if (IsNaM(elem))
                    return default;
                return elem.Deserialize<T>(new JsonSerializerOptions { WriteIndented = false });
            }
            return default;
        }

        public static Dictionary<string, object> GetData(string path, object line)
        {
            string idStr = line.ToString();
            var cacheKey = $"{path}#{idStr}";

            if (_cache.TryGet(cacheKey, out var cachedItem))
            {
                var result = new Dictionary<string, object>();
                foreach (var kvp in cachedItem.data)
                {
                    result[kvp.Key] = kvp.Value.Deserialize<object>(new JsonSerializerOptions { WriteIndented = false });
                }
                return result;
            }

            var settings = ReadSettings(path);
            var headers = ReadHeaders(path, settings.Separator);
            var data = new Dictionary<string, object>();

            using (var reader = new StreamReader(path))
            {
                reader.ReadLine(); // Skip settings
                reader.ReadLine(); // Skip headers

                string fileLine;
                while ((fileLine = reader.ReadLine()) != null)
                {
                    var parts = fileLine.Split(settings.Separator);
                    string encodedId = parts[0];
                    string decodedId = DecodeBase64(encodedId);
                    if (decodedId == idStr)
                    {
                        for (int i = 0; i < headers.Count; i++)
                        {
                            string header = headers[i];
                            string encodedValue = parts[i];
                            string decodedValue = DecodeBase64(encodedValue);
                            if (decodedValue == NaM)
                            {
                                data[header] = null;
                                continue;
                            }

                            try
                            {
                                using var doc = JsonDocument.Parse(decodedValue);
                                var value = doc.RootElement.Deserialize<object>(new JsonSerializerOptions { WriteIndented = false });
                                data[header] = value;
                            }
                            catch
                            {
                                data[header] = null;
                            }
                        }
                        break;
                    }
                }
            }

            var newCacheItem = new CacheItem
            {
                data = new Dictionary<string, JsonElement>(),
                last_modified = DateTime.Now
            };
            foreach (var kvp in data)
            {
                string json = JsonSerializer.Serialize(kvp.Value, new JsonSerializerOptions { WriteIndented = false });
                using var doc = JsonDocument.Parse(json);
                newCacheItem.data[kvp.Key] = doc.RootElement.Clone();
            }
            _cache.AddOrUpdate(cacheKey, newCacheItem);

            return data;
        }

        private static string GenerateNextId(string path, TableSettings settings)
        {
            int maxId = 0;
            using (var reader = new StreamReader(path))
            {
                reader.ReadLine(); // Skip settings
                reader.ReadLine(); // Skip headers

                string line;
                while ((line = reader.ReadLine()) != null)
                {
                    var parts = line.Split(settings.Separator);
                    string encodedId = parts[0];
                    string decodedId = DecodeBase64(encodedId);
                    if (int.TryParse(decodedId, out int id))
                    {
                        if (id > maxId)
                            maxId = id;
                    }
                }
            }
            int nextId = maxId + 1;
            return Convert.ToBase64String(Encoding.UTF8.GetBytes(nextId.ToString()));
        }

        private static TableSettings ReadSettings(string path)
        {
            string firstLine = File.ReadLines(path).First();
            var settings = new TableSettings();
            foreach (var part in firstLine.Split(';'))
            {
                var keyValue = part.Split(':');
                if (keyValue.Length == 2)
                {
                    switch (keyValue[0])
                    {
                        case "KeyRow":
                            settings.KeyRow = keyValue[1];
                            break;
                        case "Separator":
                            settings.Separator = keyValue[1];
                            break;
                        case "DankVersion":
                            settings.DankVersion = keyValue[1];
                            break;
                    }
                }
            }
            return settings;
        }

        private static List<string> ReadHeaders(string path, string separator)
        {
            var lines = File.ReadLines(path);
            var secondLine = lines.Skip(1).First();
            return secondLine.Split(new[] { separator }, StringSplitOptions.None).ToList();
        }

        private static string DecodeBase64(string encoded)
        {
            if (encoded == NaM)
                return NaM;
            try
            {
                byte[] data = Convert.FromBase64String(encoded);
                return Encoding.UTF8.GetString(data);
            }
            catch
            {
                return NaM;
            }
        }

        private static bool IsNaM(JsonElement element)
        {
            return element.ValueKind == JsonValueKind.String && element.GetString() == NaM;
        }

        private class TableSettings
        {
            public string KeyRow { get; set; }
            public string Separator { get; set; }
            public string DankVersion { get; set; }
        }
    }

    public class CacheItem
    {
        public Dictionary<string, JsonElement> data { get; set; } = new Dictionary<string, JsonElement>();
        public DateTime last_modified { get; set; }
    }

    public sealed class LruCache<TKey, TValue> where TKey : notnull
    {
        private readonly ConcurrentDictionary<TKey, LinkedListNode<LruCacheItem>> cache = new();
        private readonly LinkedList<LruCacheItem> lru = new();
        public int capacity { get; set; }
        public int count => cache.Count;

        public LruCache(int capacity)
        {
            this.capacity = capacity;
        }

        public TValue GetOrAdd(TKey key, Func<TKey, TValue> valueFactory)
        {
            var node = cache.GetOrAdd(key, k =>
            {
                var item = new LruCacheItem(k, valueFactory(k));
                return new LinkedListNode<LruCacheItem>(item);
            });

            lock (lru)
            {
                if (node.List == lru)
                    lru.Remove(node);

                lru.AddFirst(node);
            }

            MaintainCapacity();
            return node.Value.Value;
        }

        public async Task<TValue> GetOrAddAsync(TKey key, Func<TKey, Task<TValue>> valueFactory)
        {
            if (cache.TryGetValue(key, out var existingNode))
            {
                lock (lru)
                {
                    lru.Remove(existingNode);
                    lru.AddFirst(existingNode);
                }
                return existingNode.Value.Value;
            }

            var value = await valueFactory(key);
            var newNode = new LinkedListNode<LruCacheItem>(new LruCacheItem(key, value));

            lock (lru)
            {
                if (cache.TryAdd(key, newNode))
                {
                    lru.AddFirst(newNode);
                }
                else
                {
                    lru.AddFirst(cache[key]);
                }
            }

            MaintainCapacity();
            return value;
        }

        public void AddOrUpdate(TKey key, TValue value)
        {
            var newNode = new LinkedListNode<LruCacheItem>(new LruCacheItem(key, value));

            lock (lru)
            {
                if (cache.TryGetValue(key, out var oldNode))
                {
                    lru.Remove(oldNode);
                }

                cache[key] = newNode;
                lru.AddFirst(newNode);
                MaintainCapacity();
            }
        }

        public bool TryGet(TKey key, out TValue value)
        {
            if (cache.TryGetValue(key, out var node))
            {
                value = node.Value.Value;
                lock (lru)
                {
                    lru.Remove(node);
                    lru.AddFirst(node);
                }
                return true;
            }
            value = default;
            return false;
        }

        public void Refresh(TKey key)
        {
            if (cache.TryGetValue(key, out var node))
            {
                lock (lru)
                {
                    lru.Remove(node);
                    lru.AddFirst(node);
                }
            }
        }

        public void Invalidate(TKey key)
        {
            lock (lru)
            {
                if (cache.TryRemove(key, out var node))
                    lru.Remove(node);
            }
        }

        public void Clear()
        {
            lock (lru)
            {
                cache.Clear();
                lru.Clear();
            }
        }

        private void MaintainCapacity()
        {
            while (cache.Count > capacity)
            {
                lock (lru)
                {
                    if (cache.Count <= capacity) break;

                    var lastNode = lru.Last;
                    cache.TryRemove(lastNode.Value.Key, out _);
                    lru.RemoveLast();
                }
            }
        }

        private sealed class LruCacheItem
        {
            public TKey Key { get; }
            public TValue Value { get; }

            public LruCacheItem(TKey key, TValue value)
            {
                Key = key;
                Value = value;
            }
        }
    }
}