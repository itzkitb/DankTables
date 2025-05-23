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
        private static readonly LruCache<string, TableData> cache = new LruCache<string, TableData>(100);
        private static string version = "1.0";
        public const string libVersion = "1.0.1";
        private static List<string> suportedVersion = new() { "1.0" };

        private class TableData
        {
            public Dictionary<string, string> Settings { get; set; } = new();
            public List<string> Rows { get; set; } = new();
            public List<Dictionary<string, object>> Lines { get; set; } = new();
        }

        public static void CreateDatabase(string path, List<string> rows, string mainRow)
        {
            if (!rows.Contains(mainRow))
                throw new ArgumentException("Main row must be in rows list");

            foreach (var row in rows)
            {
                if (!Regex.IsMatch(row, @"^[a-zA-Z]+$"))
                    throw new ArgumentException($"Row name '{row}' contains non-English characters");
            }

            var settings = $"KeyRow:{mainRow};Separator:|;DankVersion:{version};";
            var header = string.Join("|", rows);

            File.WriteAllLines(path, new[] { settings, header });
            cache.Invalidate(path);
        }

        public static void AddRow(string path, string row)
        {
            if (!Regex.IsMatch(row, @"^[a-zA-Z]+$"))
                throw new ArgumentException("Row name must be in English");

            var table = LoadTable(path);

            if (table.Rows.Contains(row))
                throw new ArgumentException("Row already exists");

            table.Rows.Add(row);

            foreach (var line in table.Lines)
            {
                if (!line.ContainsKey(row))
                    line[row] = NaM;
            }

            SaveTable(path, table);
        }

        public static void AddRows(string path, List<string> rows)
        {
            foreach (var row in rows)
                AddRow(path, row);
        }

        public static void RemoveRow(string path, string row)
        {
            var table = LoadTable(path);
            var keyRow = table.Settings["KeyRow"];

            if (row == keyRow)
                throw new ArgumentException("Cannot remove key row");

            if (!table.Rows.Remove(row))
                throw new ArgumentException("Row does not exist");

            foreach (var line in table.Lines)
                line.Remove(row);

            SaveTable(path, table);
        }

        public static void RemoveRows(string path, List<string> rows)
        {
            foreach (var row in rows)
                RemoveRow(path, row);
        }

        public static void AddLine(string path, Dictionary<string, object> data)
        {
            var table = LoadTable(path);
            var keyRow = table.Settings["KeyRow"];

            if (!data.ContainsKey(keyRow))
                throw new ArgumentException($"Data must contain key row '{keyRow}'");

            var newLine = new Dictionary<string, object>();
            foreach (var row in table.Rows)
            {
                if (data.TryGetValue(row, out var value))
                    newLine[row] = value;
                else
                    newLine[row] = NaM;
            }

            table.Lines.Add(newLine);
            SaveTable(path, table);
        }

        public static void RemoveLine(string path, object id)
        {
            var table = LoadTable(path);
            var keyRow = table.Settings["KeyRow"];

            var lineToRemove = table.Lines.FirstOrDefault(l =>
                l[keyRow]?.ToString() == id.ToString());

            if (lineToRemove != null)
            {
                table.Lines.Remove(lineToRemove);
                SaveTable(path, table);
            }
        }

        public static void EditData(string path, object line, string row, object data)
        {
            var table = LoadTable(path);
            var keyRow = table.Settings["KeyRow"];

            var targetLine = table.Lines.FirstOrDefault(l =>
                l[keyRow]?.ToString() == line.ToString());

            if (targetLine == null)
                throw new ArgumentException("Line not found");

            if (!table.Rows.Contains(row))
                throw new ArgumentException($"Row '{row}' does not exist");

            targetLine[row] = data;
            SaveTable(path, table);
        }

        public static T GetData<T>(string path, object line, string row)
        {
            var table = LoadTable(path);
            var keyRow = table.Settings["KeyRow"];

            var targetLine = table.Lines.FirstOrDefault(l =>
                l[keyRow]?.ToString() == line.ToString());

            if (targetLine == null || !targetLine.TryGetValue(row, out var value))
                return default;

            if (value is null || value.ToString() == NaM)
                return default;

            try
            {
                return (T)value;
            }
            catch
            {
                return JsonSerializer.Deserialize<T>(JsonSerializer.SerializeToElement(value));
            }
        }

        public static Dictionary<string, object> GetData(string path, object line)
        {
            var table = LoadTable(path);
            var keyRow = table.Settings["KeyRow"];

            var targetLine = table.Lines.FirstOrDefault(l =>
                l[keyRow]?.ToString() == line.ToString());

            if (targetLine == null)
                throw new ArgumentException("Line not found");

            return new Dictionary<string, object>(targetLine);
        }

        public static Dictionary<object, Dictionary<string, object>> GetData(string path)
        {
            var table = LoadTable(path);
            var keyRow = table.Settings["KeyRow"];

            return table.Lines.ToDictionary(
                l => l[keyRow],
                l => new Dictionary<string, object>(l)
            );
        }

        private static TableData LoadTable(string path)
        {
            if (cache.TryGet(path, out var table))
                return table;

            var lines = File.ReadAllLines(path);
            var tableData = new TableData();

            var settings = lines[0].Split(new[] { ';', ':' }, StringSplitOptions.RemoveEmptyEntries);
            for (int i = 0; i < settings.Length; i += 2)
                tableData.Settings[settings[i]] = settings[i + 1];

            if (!suportedVersion.Contains(tableData.Settings["DankVersion"]))
                throw new InvalidOperationException($"Unsupported version of DankTable. Current version: {version}. Supported versions: {string.Join(",", suportedVersion)}. File version: {tableData.Settings["DankVersion"]}. Library version: {libVersion}. Try updating the library to the latest version.");

            char Separator = tableData.Settings["Separator"] is not null ? tableData.Settings["Separator"].ToArray()[0] : '|';

            tableData.Rows = lines[1].Split(Separator).ToList();
            foreach (var row in tableData.Rows)
            {
                if (!Regex.IsMatch(row, @"^[a-zA-Z]+$"))
                    throw new ArgumentException($"Row name '{row}' contains non-English characters");
            }

            for (int i = 2; i < lines.Length; i++)
            {
                var cells = lines[i].Split(Separator);
                var lineData = new Dictionary<string, object>();

                for (int j = 0; j < cells.Length && j < tableData.Rows.Count; j++)
                {
                    var decoded = Encoding.UTF8.GetString(Convert.FromBase64String(cells[j]));
                    if (decoded == NaM)
                        lineData[tableData.Rows[j]] = NaM;
                    else
                        lineData[tableData.Rows[j]] = JsonSerializer.Deserialize<object>(decoded);
                }

                tableData.Lines.Add(lineData);
            }

            cache.AddOrUpdate(path, tableData);
            return tableData;
        }

        private static void SaveTable(string path, TableData table)
        {
            var outputLines = new List<string>
            {
                $"KeyRow:{table.Settings["KeyRow"]};Separator:|;DankVersion:{version};",
                string.Join("|", table.Rows)
            };

            foreach (var line in table.Lines)
            {
                var cells = new List<string>();
                foreach (var row in table.Rows)
                {
                    if (line.TryGetValue(row, out var value))
                    {
                        if (value.ToString() == NaM)
                        {
                            cells.Add(Convert.ToBase64String(Encoding.UTF8.GetBytes(NaM)));
                        }
                        else
                        {
                            var json = JsonSerializer.Serialize(value);
                            cells.Add(Convert.ToBase64String(Encoding.UTF8.GetBytes(json)));
                        }
                    }
                    else
                    {
                        cells.Add(Convert.ToBase64String(Encoding.UTF8.GetBytes(NaM)));
                    }
                }
                outputLines.Add(string.Join("|", cells));
            }

            File.WriteAllLines(path, outputLines);
            cache.AddOrUpdate(path, table);
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

        public IEnumerable<TKey> GetKeys()
        {
            return cache.Keys;
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