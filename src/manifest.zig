const std = @import("std");
const Allocator = std.mem.Allocator;

// ============================================================================
// EXIM Manifest - shared between planck (storage layer) and workbench
//
// Export produces a manifest describing the exported file structure.
// Import consumes a manifest (import spec) describing how to reassemble
// flat files into nested documents.
// ============================================================================

/// Field data type for CSV column mapping
pub const FieldType = enum(u8) {
    string = 1,
    int = 2,
    double = 3,
    bool = 4,
    datetime = 5,
    objectid = 6,

    pub fn fromString(s: []const u8) ?FieldType {
        if (std.mem.eql(u8, s, "string")) return .string;
        if (std.mem.eql(u8, s, "int")) return .int;
        if (std.mem.eql(u8, s, "double")) return .double;
        if (std.mem.eql(u8, s, "bool")) return .bool;
        if (std.mem.eql(u8, s, "datetime")) return .datetime;
        if (std.mem.eql(u8, s, "objectid")) return .objectid;
        return null;
    }

    pub fn toString(self: FieldType) []const u8 {
        return switch (self) {
            .string => "string",
            .int => "int",
            .double => "double",
            .bool => "bool",
            .datetime => "datetime",
            .objectid => "objectid",
        };
    }
};

/// Role of a file in the manifest hierarchy
pub const FileRole = enum(u8) {
    parent = 1,
    child = 2,

    pub fn fromString(s: []const u8) ?FileRole {
        if (std.mem.eql(u8, s, "parent")) return .parent;
        if (std.mem.eql(u8, s, "child")) return .child;
        return null;
    }

    pub fn toString(self: FileRole) []const u8 {
        return switch (self) {
            .parent => "parent",
            .child => "child",
        };
    }
};

/// Export format
pub const ExportFormat = enum(u8) {
    bson = 1,
    json = 2,
    csv = 3,

    pub fn fromString(s: []const u8) ?ExportFormat {
        if (std.mem.eql(u8, s, "bson")) return .bson;
        if (std.mem.eql(u8, s, "json")) return .json;
        if (std.mem.eql(u8, s, "csv")) return .csv;
        return null;
    }

    pub fn toString(self: ExportFormat) []const u8 {
        return switch (self) {
            .bson => "bson",
            .json => "json",
            .csv => "csv",
        };
    }
};

/// A field descriptor used in import specs for CSV type mapping
pub const FieldDescriptor = struct {
    name: []const u8,
    field_type: FieldType,
};

// ============================================================================
// Export Manifest - written by exporter alongside exported files
// ============================================================================

/// Describes one exported file in the manifest
pub const ExportFileEntry = struct {
    name: []const u8,
    role: FileRole,
    parent: ?[]const u8 = null, // parent file name (for child files)
    link_field: ?[]const u8 = null, // field in child that links to parent (_parent_id)
    injected_fields: ?[]const []const u8 = null, // business keys copied from parent
    fields: []const []const u8, // column names in this file
};

/// Full export manifest - describes the structure of an export
pub const ExportManifest = struct {
    source: []const u8, // source store namespace (e.g. "sales.orders")
    format: ExportFormat,
    files: []const ExportFileEntry,

    pub fn deinit(self: *ExportManifest, allocator: Allocator) void {
        for (self.files) |entry| {
            allocator.free(entry.name);
            if (entry.parent) |p| allocator.free(p);
            if (entry.link_field) |lf| allocator.free(lf);
            if (entry.injected_fields) |ifs| {
                for (ifs) |f| allocator.free(f);
                allocator.free(ifs);
            }
            for (entry.fields) |f| allocator.free(f);
            allocator.free(entry.fields);
        }
        allocator.free(self.files);
        allocator.free(self.source);
    }
};

// ============================================================================
// Import Spec - consumed by importer to know how to reassemble documents
// ============================================================================

/// Describes one source file in the import spec
pub const ImportSourceEntry = struct {
    name: ?[]const u8 = null, // entity name (e.g. "orders", "details")
    file: []const u8, // file path
    role: FileRole,
    parent: ?[]const u8 = null, // parent entity name (for multi-level nesting)
    embed_as: ?[]const u8 = null, // array field name to embed children under
    join_key: ?[]const u8 = null, // field that links child rows to parent
    fields: []const FieldDescriptor, // column definitions with types
};

/// Full import spec - describes how to import data into a store
pub const ImportSpec = struct {
    target: []const u8, // target store namespace (e.g. "sales.orders")
    format: ExportFormat,
    file_path: ?[]const u8 = null, // for bson/json: single source file
    sources: ?[]const ImportSourceEntry = null, // for csv: multiple source files
    fields: ?[]const FieldDescriptor = null, // for json: optional type hints per field

    pub fn deinit(self: *ImportSpec, allocator: Allocator) void {
        allocator.free(self.target);
        if (self.file_path) |fp| allocator.free(fp);
        if (self.fields) |fields| {
            for (fields) |fd| allocator.free(fd.name);
            allocator.free(fields);
        }
        if (self.sources) |sources| {
            for (sources) |entry| {
                if (entry.name) |n| allocator.free(n);
                allocator.free(entry.file);
                if (entry.parent) |p| allocator.free(p);
                if (entry.embed_as) |ea| allocator.free(ea);
                if (entry.join_key) |jk| allocator.free(jk);
                for (entry.fields) |fd| allocator.free(fd.name);
                allocator.free(entry.fields);
            }
            allocator.free(sources);
        }
    }

    /// Find the parent source entry
    pub fn findParent(self: *const ImportSpec) ?*const ImportSourceEntry {
        if (self.sources) |sources| {
            for (sources) |*entry| {
                if (entry.role == .parent) return entry;
            }
        }
        return null;
    }

    /// Find child entries that belong to a given parent entity name.
    pub fn findChildren(self: *const ImportSpec, allocator: Allocator, parent_name: []const u8) ![]const *const ImportSourceEntry {
        var result = std.ArrayList(*const ImportSourceEntry).empty;
        errdefer result.deinit(allocator);

        if (self.sources) |sources| {
            for (sources) |*entry| {
                if (entry.role != .child) continue;

                if (entry.parent) |p| {
                    // Explicit parent - match by entity name
                    if (std.mem.eql(u8, p, parent_name)) {
                        try result.append(allocator, entry);
                    }
                } else {
                    // No explicit parent - direct child of the root parent entity
                    if (self.findParent()) |pe| {
                        if (pe.name) |pn| {
                            if (std.mem.eql(u8, pn, parent_name)) {
                                try result.append(allocator, entry);
                            }
                        }
                    }
                }
            }
        }

        return try result.toOwnedSlice(allocator);
    }

    /// Determine the processing order (bottom-up: deepest children first)
    pub fn buildOrder(self: *const ImportSpec, allocator: Allocator) ![]const *const ImportSourceEntry {
        if (self.sources == null) return &[_]*const ImportSourceEntry{};

        var ordered = std.ArrayList(*const ImportSourceEntry).empty;
        errdefer ordered.deinit(allocator);

        var visited = std.StringHashMap(bool).init(allocator);
        defer visited.deinit();

        // Recursive depth-first: process children before parents
        if (self.findParent()) |parent_entry| {
            try self.buildOrderRecursive(allocator, parent_entry, &ordered, &visited);
        }

        return try ordered.toOwnedSlice(allocator);
    }

    fn buildOrderRecursive(
        self: *const ImportSpec,
        allocator: Allocator,
        entry: *const ImportSourceEntry,
        ordered: *std.ArrayList(*const ImportSourceEntry),
        visited: *std.StringHashMap(bool),
    ) !void {
        const entry_name = entry.name orelse entry.file;
        if (visited.contains(entry_name)) return;

        // Process children first (depth-first)
        const children = try self.findChildren(allocator, entry_name);
        defer allocator.free(children);

        for (children) |child| {
            try self.buildOrderRecursive(allocator, child, ordered, visited);
        }

        // Then add this entry
        try ordered.append(allocator, entry);
        try visited.put(entry_name, true);
    }
};

// ============================================================================
// EXIM Manifest - user-authored YAML input for both export and import
//
// Parsed from uploaded YAML. Contains store, format, output_dir, optional
// query filter (export only), and entity definitions with fields, hierarchy,
// and file mappings. Used by both wb and db via the utils dependency.
// ============================================================================

/// An entity definition within the manifest (parent or child)
pub const EntityDef = struct {
    name: []const u8,
    role: FileRole,
    file: []const u8,
    parent: ?[]const u8 = null, // parent entity name (child only)
    parent_field: ?[]const u8 = null, // array field name in parent doc (child only)
    join_key: ?[]const u8 = null, // parent field injected into child rows (child only)
    fields: []const FieldDescriptor,
};

/// The unified manifest parsed from user-uploaded YAML.
/// Works for both export and import across all formats.
pub const EximManifest = struct {
    store: []const u8, // store namespace (e.g. "stores.orders")
    format: ExportFormat,
    output_dir: ?[]const u8 = null, // export output / import input directory
    query: ?[]const u8 = null, // export only - PQL filter text
    entities: []const EntityDef,

    pub fn deinit(self: *EximManifest, allocator: Allocator) void {
        allocator.free(self.store);
        if (self.output_dir) |od| allocator.free(od);
        if (self.query) |q| allocator.free(q);
        for (self.entities) |entity| {
            allocator.free(entity.name);
            allocator.free(entity.file);
            if (entity.parent) |p| allocator.free(p);
            if (entity.parent_field) |pf| allocator.free(pf);
            if (entity.join_key) |jk| allocator.free(jk);
            for (entity.fields) |fd| allocator.free(fd.name);
            allocator.free(entity.fields);
        }
        allocator.free(self.entities);
    }

    /// Find the root parent entity
    pub fn findRoot(self: *const EximManifest) ?*const EntityDef {
        for (self.entities) |*e| {
            if (e.role == .parent) return e;
        }
        return null;
    }

    /// Find direct children of a given entity name
    pub fn findChildren(self: *const EximManifest, allocator: Allocator, parent_name: []const u8) ![]const *const EntityDef {
        var result = std.ArrayList(*const EntityDef).empty;
        errdefer result.deinit(allocator);

        const root = self.findRoot();

        for (self.entities) |*e| {
            if (e.role != .child) continue;
            if (e.parent) |p| {
                if (std.mem.eql(u8, p, parent_name)) {
                    try result.append(allocator, e);
                }
            } else {
                // No explicit parent → direct child of root
                if (root) |r| {
                    if (std.mem.eql(u8, r.name, parent_name)) {
                        try result.append(allocator, e);
                    }
                }
            }
        }

        return try result.toOwnedSlice(allocator);
    }

    /// Convert to ImportSpec for use with existing import.zig.
    /// For BSON/JSON: sets file_path from root entity's file (or output_dir/file).
    /// For CSV: builds sources array from all entities (empty array if none).
    pub fn toImportSpec(self: *const EximManifest, allocator: Allocator) !ImportSpec {
        const target = try allocator.dupe(u8, self.store);
        errdefer allocator.free(target);

        // For BSON/JSON, the importer uses file_path (single file), not sources.
        // When entities is empty, `findRoot()` returns null and we fall through
        // to the format's default file name - so this branch covers both the
        // entity-driven and empty-entities cases coherently.
        if (self.format == .bson or self.format == .json) {
            const root = self.findRoot();
            const file_name = if (root) |r| r.file else switch (self.format) {
                .bson => "export.bson",
                .json => "export.json",
                else => unreachable,
            };
            const file_path = if (self.output_dir) |od|
                try std.fmt.allocPrint(allocator, "{s}/{s}", .{ od, file_name })
            else
                try allocator.dupe(u8, file_name);

            return ImportSpec{
                .target = target,
                .format = self.format,
                .file_path = file_path,
            };
        }

        // CSV: build sources from entities
        var sources = std.ArrayList(ImportSourceEntry).empty;
        errdefer {
            for (sources.items) |entry| {
                if (entry.name) |n| allocator.free(n);
                allocator.free(entry.file);
                if (entry.parent) |p| allocator.free(p);
                if (entry.embed_as) |ea| allocator.free(ea);
                if (entry.join_key) |jk| allocator.free(jk);
                for (entry.fields) |fd| allocator.free(fd.name);
                allocator.free(entry.fields);
            }
            sources.deinit(allocator);
        }

        for (self.entities) |entity| {
            // Build file path: output_dir/file or just file
            const file_path = if (self.output_dir) |od| blk: {
                break :blk try std.fmt.allocPrint(allocator, "{s}/{s}", .{ od, entity.file });
            } else try allocator.dupe(u8, entity.file);

            // Copy fields
            var fields = try allocator.alloc(FieldDescriptor, entity.fields.len);
            for (entity.fields, 0..) |fd, i| {
                fields[i] = .{
                    .name = try allocator.dupe(u8, fd.name),
                    .field_type = fd.field_type,
                };
            }

            try sources.append(allocator, .{
                .name = try allocator.dupe(u8, entity.name),
                .file = file_path,
                .role = entity.role,
                .parent = if (entity.parent) |p| try allocator.dupe(u8, p) else null,
                .embed_as = if (entity.parent_field) |pf| try allocator.dupe(u8, pf) else null,
                .join_key = if (entity.join_key) |jk| try allocator.dupe(u8, jk) else null,
                .fields = fields,
            });
        }

        return ImportSpec{
            .target = target,
            .format = self.format,
            .sources = try sources.toOwnedSlice(allocator),
        };
    }
};

// ============================================================================
// YAML Manifest Parser
//
// Simple line-by-line parser for the EXIM manifest format. Not a general YAML
// parser - only handles the known manifest structure (top-level scalars,
// entities array with nested fields array).
// ============================================================================

pub const ManifestParseError = error{
    MissingStore,
    MissingFormat,
    InvalidFormat,
    InvalidRole,
    InvalidFieldType,
    MissingEntityName,
    MissingEntityRole,
    MissingEntityFile,
    OutOfMemory,
};

/// Parse a YAML manifest string into an EximManifest.
/// Caller owns the returned manifest and must call deinit().
pub fn parseManifestYaml(allocator: Allocator, yaml: []const u8) ManifestParseError!EximManifest {
    var store: ?[]const u8 = null;
    var format: ?ExportFormat = null;
    var output_dir: ?[]const u8 = null;
    var query: ?[]const u8 = null;

    errdefer {
        if (store) |s| allocator.free(s);
        if (output_dir) |od| allocator.free(od);
        if (query) |q| allocator.free(q);
    }

    var entities = std.ArrayList(EntityDef).empty;
    errdefer {
        for (entities.items) |entity| {
            allocator.free(entity.name);
            allocator.free(entity.file);
            if (entity.parent) |p| allocator.free(p);
            if (entity.parent_field) |pf| allocator.free(pf);
            if (entity.join_key) |jk| allocator.free(jk);
            for (entity.fields) |fd| allocator.free(fd.name);
            allocator.free(entity.fields);
        }
        entities.deinit(allocator);
    }

    // Track parsing state
    const State = enum { top, entity, fields };
    var state: State = .top;

    // Current entity being built
    var cur_name: ?[]const u8 = null;
    var cur_role: ?FileRole = null;
    var cur_file: ?[]const u8 = null;
    var cur_parent: ?[]const u8 = null;
    var cur_parent_field: ?[]const u8 = null;
    var cur_join_key: ?[]const u8 = null;
    var cur_fields = std.ArrayList(FieldDescriptor).empty;

    errdefer {
        if (cur_name) |n| allocator.free(n);
        if (cur_file) |f| allocator.free(f);
        if (cur_parent) |p| allocator.free(p);
        if (cur_parent_field) |pf| allocator.free(pf);
        if (cur_join_key) |jk| allocator.free(jk);
        for (cur_fields.items) |fd| allocator.free(fd.name);
        cur_fields.deinit(allocator);
    }

    var lines = std.mem.splitScalar(u8, yaml, '\n');

    while (lines.next()) |raw_line| {
        const line = std.mem.trimEnd(u8, raw_line, &[_]u8{ '\r', ' ', '\t' });
        if (line.len == 0) continue;

        // Count leading spaces
        const indent = countIndent(line);
        const trimmed = std.mem.trimStart(u8, line, &[_]u8{ ' ', '\t' });

        // Skip comments
        if (trimmed.len > 0 and trimmed[0] == '#') continue;

        switch (state) {
            .top => {
                if (std.mem.startsWith(u8, trimmed, "entities:")) {
                    state = .entity;
                    continue;
                }
                if (parseScalar(trimmed, "store")) |v| {
                    store = allocator.dupe(u8, v) catch return ManifestParseError.OutOfMemory;
                } else if (parseScalar(trimmed, "format")) |v| {
                    format = ExportFormat.fromString(v) orelse return ManifestParseError.InvalidFormat;
                } else if (parseScalar(trimmed, "output_dir")) |v| {
                    output_dir = allocator.dupe(u8, v) catch return ManifestParseError.OutOfMemory;
                } else if (parseScalar(trimmed, "query")) |v| {
                    query = allocator.dupe(u8, unquote(v)) catch return ManifestParseError.OutOfMemory;
                }
            },
            .entity => {
                // "- name: xxx" starts a new entity (indent 2+, starts with "- ")
                if (std.mem.startsWith(u8, trimmed, "- ")) {
                    // Flush previous entity if any
                    try flushEntity(allocator, &entities, &cur_name, &cur_role, &cur_file, &cur_parent, &cur_parent_field, &cur_join_key, &cur_fields);

                    // Parse "- name: xxx" or "- { name: xxx, ... }" (inline)
                    const after_dash = std.mem.trimStart(u8, trimmed[2..], &[_]u8{ ' ', '\t' });
                    if (after_dash.len > 0 and after_dash[0] == '{') {
                        // Inline form: - { name: sales, role: parent, file: sales.csv, fields: [...] }
                        try parseInlineEntity(allocator, after_dash, &entities);
                        continue;
                    }
                    // "- name: xxx"
                    if (parseScalar(after_dash, "name")) |v| {
                        cur_name = allocator.dupe(u8, v) catch return ManifestParseError.OutOfMemory;
                    }
                    continue;
                }

                // Entity-level fields (indent 4+) or fields array
                if (indent >= 4) {
                    if (std.mem.startsWith(u8, trimmed, "fields:")) {
                        state = .fields;
                        continue;
                    }
                    if (parseScalar(trimmed, "name")) |v| {
                        if (cur_name) |old| allocator.free(old);
                        cur_name = allocator.dupe(u8, v) catch return ManifestParseError.OutOfMemory;
                    } else if (parseScalar(trimmed, "role")) |v| {
                        cur_role = FileRole.fromString(v) orelse return ManifestParseError.InvalidRole;
                    } else if (parseScalar(trimmed, "file")) |v| {
                        if (cur_file) |old| allocator.free(old);
                        cur_file = allocator.dupe(u8, v) catch return ManifestParseError.OutOfMemory;
                    } else if (parseScalar(trimmed, "parent")) |v| {
                        if (cur_parent) |old| allocator.free(old);
                        cur_parent = allocator.dupe(u8, v) catch return ManifestParseError.OutOfMemory;
                    } else if (parseScalar(trimmed, "parent_field")) |v| {
                        if (cur_parent_field) |old| allocator.free(old);
                        cur_parent_field = allocator.dupe(u8, v) catch return ManifestParseError.OutOfMemory;
                    } else if (parseScalar(trimmed, "join_key")) |v| {
                        if (cur_join_key) |old| allocator.free(old);
                        cur_join_key = allocator.dupe(u8, v) catch return ManifestParseError.OutOfMemory;
                    }
                    continue;
                }

                // Back to top-level - shouldn't happen normally in valid YAML
            },
            .fields => {
                // Check indent first - if we've left the fields block, transition back
                if (indent < 6) {
                    state = .entity;
                    // Re-process this line as entity-level
                    if (std.mem.startsWith(u8, trimmed, "- ")) {
                        try flushEntity(allocator, &entities, &cur_name, &cur_role, &cur_file, &cur_parent, &cur_parent_field, &cur_join_key, &cur_fields);
                        const after_dash = std.mem.trimStart(u8, trimmed[2..], &[_]u8{ ' ', '\t' });
                        if (parseScalar(after_dash, "name")) |v| {
                            cur_name = allocator.dupe(u8, v) catch return ManifestParseError.OutOfMemory;
                        }
                    } else if (parseScalar(trimmed, "name")) |v| {
                        if (cur_name) |old| allocator.free(old);
                        cur_name = allocator.dupe(u8, v) catch return ManifestParseError.OutOfMemory;
                    } else if (parseScalar(trimmed, "role")) |v| {
                        cur_role = FileRole.fromString(v) orelse return ManifestParseError.InvalidRole;
                    } else if (parseScalar(trimmed, "file")) |v| {
                        if (cur_file) |old| allocator.free(old);
                        cur_file = allocator.dupe(u8, v) catch return ManifestParseError.OutOfMemory;
                    } else if (parseScalar(trimmed, "parent")) |v| {
                        if (cur_parent) |old| allocator.free(old);
                        cur_parent = allocator.dupe(u8, v) catch return ManifestParseError.OutOfMemory;
                    } else if (parseScalar(trimmed, "parent_field")) |v| {
                        if (cur_parent_field) |old| allocator.free(old);
                        cur_parent_field = allocator.dupe(u8, v) catch return ManifestParseError.OutOfMemory;
                    } else if (parseScalar(trimmed, "join_key")) |v| {
                        if (cur_join_key) |old| allocator.free(old);
                        cur_join_key = allocator.dupe(u8, v) catch return ManifestParseError.OutOfMemory;
                    }
                    continue;
                }

                // Still in fields block (indent >= 6) - parse field entries
                if (std.mem.startsWith(u8, trimmed, "- ")) {
                    const after_dash = std.mem.trimStart(u8, trimmed[2..], &[_]u8{ ' ', '\t' });

                    if (after_dash.len > 0 and after_dash[0] == '{') {
                        // Inline: - { name: sku, type: string }
                        try parseInlineField(allocator, after_dash, &cur_fields);
                    } else if (parseScalar(after_dash, "name")) |v| {
                        // Block form: "- name: xxx" followed by "  type: yyy"
                        const field_name = allocator.dupe(u8, v) catch return ManifestParseError.OutOfMemory;
                        var field_type: FieldType = .string; // default

                        if (lines.next()) |next_raw| {
                            const next_line = std.mem.trimEnd(u8, next_raw, &[_]u8{ '\r', ' ', '\t' });
                            const next_trimmed = std.mem.trimStart(u8, next_line, &[_]u8{ ' ', '\t' });
                            if (parseScalar(next_trimmed, "type")) |tv| {
                                field_type = FieldType.fromString(tv) orelse .string;
                            }
                        }

                        cur_fields.append(allocator, .{
                            .name = field_name,
                            .field_type = field_type,
                        }) catch {
                            allocator.free(field_name);
                            return ManifestParseError.OutOfMemory;
                        };
                    }
                }
            },
        }
    }

    // Flush last entity
    try flushEntity(allocator, &entities, &cur_name, &cur_role, &cur_file, &cur_parent, &cur_parent_field, &cur_join_key, &cur_fields);

    // Validate required fields
    const final_store = store orelse return ManifestParseError.MissingStore;
    const final_format = format orelse return ManifestParseError.MissingFormat;

    return EximManifest{
        .store = final_store,
        .format = final_format,
        .output_dir = output_dir,
        .query = query,
        .entities = entities.toOwnedSlice(allocator) catch return ManifestParseError.OutOfMemory,
    };
}

// ── Parser helpers ──────────────────────────────────────────────────────

fn countIndent(line: []const u8) usize {
    var n: usize = 0;
    for (line) |c| {
        if (c == ' ') {
            n += 1;
        } else if (c == '\t') {
            n += 2;
        } else break;
    }
    return n;
}

/// Extract value from "key: value" if key matches. Returns the trimmed value or null.
fn parseScalar(line: []const u8, key: []const u8) ?[]const u8 {
    if (!std.mem.startsWith(u8, line, key)) return null;
    if (line.len <= key.len) return null;
    if (line[key.len] != ':') return null;
    const after_colon = line[key.len + 1 ..];
    const trimmed = std.mem.trimStart(u8, after_colon, &[_]u8{ ' ', '\t' });
    if (trimmed.len == 0) return null;
    return trimmed;
}

/// Remove surrounding quotes from a string value
fn unquote(s: []const u8) []const u8 {
    if (s.len >= 2) {
        if ((s[0] == '"' and s[s.len - 1] == '"') or
            (s[0] == '\'' and s[s.len - 1] == '\''))
        {
            return s[1 .. s.len - 1];
        }
    }
    return s;
}

/// Flush accumulated entity fields into the entities list
fn flushEntity(
    allocator: Allocator,
    entities: *std.ArrayList(EntityDef),
    cur_name: *?[]const u8,
    cur_role: *?FileRole,
    cur_file: *?[]const u8,
    cur_parent: *?[]const u8,
    cur_parent_field: *?[]const u8,
    cur_join_key: *?[]const u8,
    cur_fields: *std.ArrayList(FieldDescriptor),
) ManifestParseError!void {
    if (cur_name.* == null) return; // nothing to flush

    const name = cur_name.* orelse return ManifestParseError.MissingEntityName;
    const role = cur_role.* orelse return ManifestParseError.MissingEntityRole;
    const file = cur_file.* orelse return ManifestParseError.MissingEntityFile;

    entities.append(allocator, .{
        .name = name,
        .role = role,
        .file = file,
        .parent = cur_parent.*,
        .parent_field = cur_parent_field.*,
        .join_key = cur_join_key.*,
        .fields = cur_fields.toOwnedSlice(allocator) catch return ManifestParseError.OutOfMemory,
    }) catch return ManifestParseError.OutOfMemory;

    // Reset state
    cur_name.* = null;
    cur_role.* = null;
    cur_file.* = null;
    cur_parent.* = null;
    cur_parent_field.* = null;
    cur_join_key.* = null;
    cur_fields.* = std.ArrayList(FieldDescriptor).empty;
}

/// Parse inline field: { name: xxx, type: yyy }
fn parseInlineField(allocator: Allocator, text: []const u8, fields: *std.ArrayList(FieldDescriptor)) ManifestParseError!void {
    // Strip braces
    const inner = blk: {
        var s = text;
        if (s.len > 0 and s[0] == '{') s = s[1..];
        if (s.len > 0 and s[s.len - 1] == '}') s = s[0 .. s.len - 1];
        break :blk std.mem.trim(u8, s, &[_]u8{ ' ', '\t' });
    };

    var name: ?[]const u8 = null;
    var field_type: FieldType = .string;

    var parts = std.mem.splitScalar(u8, inner, ',');
    while (parts.next()) |part| {
        const kv = std.mem.trim(u8, part, &[_]u8{ ' ', '\t' });
        if (parseScalar(kv, "name")) |v| {
            name = v;
        } else if (parseScalar(kv, "type")) |v| {
            field_type = FieldType.fromString(v) orelse .string;
        }
    }

    if (name) |n| {
        fields.append(allocator, .{
            .name = allocator.dupe(u8, n) catch return ManifestParseError.OutOfMemory,
            .field_type = field_type,
        }) catch return ManifestParseError.OutOfMemory;
    }
}

/// Parse inline entity: { name: xxx, role: parent, file: xxx.csv, fields: [...] }
fn parseInlineEntity(allocator: Allocator, text: []const u8, entities: *std.ArrayList(EntityDef)) ManifestParseError!void {
    // Strip braces
    const inner = blk: {
        var s = text;
        if (s.len > 0 and s[0] == '{') s = s[1..];
        if (s.len > 0 and s[s.len - 1] == '}') s = s[0 .. s.len - 1];
        break :blk std.mem.trim(u8, s, &[_]u8{ ' ', '\t' });
    };

    var name: ?[]const u8 = null;
    var role: ?FileRole = null;
    var file: ?[]const u8 = null;
    var parent: ?[]const u8 = null;
    var parent_field: ?[]const u8 = null;
    var join_key: ?[]const u8 = null;

    var parts = std.mem.splitScalar(u8, inner, ',');
    while (parts.next()) |part| {
        const kv = std.mem.trim(u8, part, &[_]u8{ ' ', '\t' });
        if (parseScalar(kv, "name")) |v| {
            name = v;
        } else if (parseScalar(kv, "role")) |v| {
            role = FileRole.fromString(v);
        } else if (parseScalar(kv, "file")) |v| {
            file = v;
        } else if (parseScalar(kv, "parent")) |v| {
            parent = v;
        } else if (parseScalar(kv, "parent_field")) |v| {
            parent_field = v;
        } else if (parseScalar(kv, "join_key")) |v| {
            join_key = v;
        }
    }

    const ename = name orelse return ManifestParseError.MissingEntityName;
    const erole = role orelse return ManifestParseError.MissingEntityRole;
    const efile = file orelse return ManifestParseError.MissingEntityFile;

    entities.append(allocator, .{
        .name = allocator.dupe(u8, ename) catch return ManifestParseError.OutOfMemory,
        .role = erole,
        .file = allocator.dupe(u8, efile) catch return ManifestParseError.OutOfMemory,
        .parent = if (parent) |p| allocator.dupe(u8, p) catch return ManifestParseError.OutOfMemory else null,
        .parent_field = if (parent_field) |pf| allocator.dupe(u8, pf) catch return ManifestParseError.OutOfMemory else null,
        .join_key = if (join_key) |jk| allocator.dupe(u8, jk) catch return ManifestParseError.OutOfMemory else null,
        .fields = &.{}, // inline entities don't carry fields
    }) catch return ManifestParseError.OutOfMemory;
}

// ============================================================================
// Tests
// ============================================================================

test "parseManifestYaml - minimal JSON manifest" {
    const allocator = std.testing.allocator;
    const yaml =
        \\store: stores.orders
        \\format: json
        \\output_dir: /data/exports
    ;

    var m = try parseManifestYaml(allocator, yaml);
    defer m.deinit(allocator);

    try std.testing.expectEqualStrings("stores.orders", m.store);
    try std.testing.expectEqual(ExportFormat.json, m.format);
    try std.testing.expectEqualStrings("/data/exports", m.output_dir.?);
    try std.testing.expectEqual(@as(?[]const u8, null), m.query);
    try std.testing.expectEqual(@as(usize, 0), m.entities.len);
}

test "parseManifestYaml - with query" {
    const allocator = std.testing.allocator;
    const yaml =
        \\store: stores.orders
        \\format: csv
        \\output_dir: /data/exports
        \\query: "orders.filter(status = \"shipped\")"
    ;

    var m = try parseManifestYaml(allocator, yaml);
    defer m.deinit(allocator);

    try std.testing.expectEqualStrings("orders.filter(status = \\\"shipped\\\")", m.query.?);
}

test "parseManifestYaml - parent only" {
    const allocator = std.testing.allocator;
    const yaml =
        \\store: stores.orders
        \\format: csv
        \\output_dir: /data/exports
        \\entities:
        \\  - name: orders
        \\    role: parent
        \\    file: orders.csv
        \\    fields:
        \\      - name: order_id
        \\        type: int
        \\      - name: customer_name
        \\        type: string
    ;

    var m = try parseManifestYaml(allocator, yaml);
    defer m.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 1), m.entities.len);
    const e = m.entities[0];
    try std.testing.expectEqualStrings("orders", e.name);
    try std.testing.expectEqual(FileRole.parent, e.role);
    try std.testing.expectEqualStrings("orders.csv", e.file);
    try std.testing.expectEqual(@as(usize, 2), e.fields.len);
    try std.testing.expectEqualStrings("order_id", e.fields[0].name);
    try std.testing.expectEqual(FieldType.int, e.fields[0].field_type);
    try std.testing.expectEqualStrings("customer_name", e.fields[1].name);
    try std.testing.expectEqual(FieldType.string, e.fields[1].field_type);
}

test "parseManifestYaml - parent + children + grandchildren" {
    const allocator = std.testing.allocator;
    const yaml =
        \\store: stores.orders
        \\format: csv
        \\output_dir: /data/exports/orders
        \\entities:
        \\  - name: orders
        \\    role: parent
        \\    file: orders.csv
        \\    fields:
        \\      - name: order_id
        \\        type: int
        \\      - name: total
        \\        type: double
        \\  - name: items
        \\    role: child
        \\    parent_field: items
        \\    join_key: order_id
        \\    file: order_items.csv
        \\    fields:
        \\      - name: item_id
        \\        type: int
        \\      - name: price
        \\        type: double
        \\  - name: attributes
        \\    role: child
        \\    parent: items
        \\    parent_field: attributes
        \\    join_key: item_id
        \\    file: item_attributes.csv
        \\    fields:
        \\      - name: attr_name
        \\        type: string
    ;

    var m = try parseManifestYaml(allocator, yaml);
    defer m.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 3), m.entities.len);

    // Parent
    try std.testing.expectEqualStrings("orders", m.entities[0].name);
    try std.testing.expectEqual(FileRole.parent, m.entities[0].role);
    try std.testing.expectEqual(@as(usize, 2), m.entities[0].fields.len);

    // Child
    try std.testing.expectEqualStrings("items", m.entities[1].name);
    try std.testing.expectEqual(FileRole.child, m.entities[1].role);
    try std.testing.expectEqualStrings("items", m.entities[1].parent_field.?);
    try std.testing.expectEqualStrings("order_id", m.entities[1].join_key.?);
    try std.testing.expectEqual(@as(?[]const u8, null), m.entities[1].parent);
    try std.testing.expectEqual(@as(usize, 2), m.entities[1].fields.len);

    // Grandchild
    try std.testing.expectEqualStrings("attributes", m.entities[2].name);
    try std.testing.expectEqualStrings("items", m.entities[2].parent.?);
    try std.testing.expectEqualStrings("attributes", m.entities[2].parent_field.?);
    try std.testing.expectEqualStrings("item_id", m.entities[2].join_key.?);
    try std.testing.expectEqual(@as(usize, 1), m.entities[2].fields.len);
}

test "parseManifestYaml - inline field syntax" {
    const allocator = std.testing.allocator;
    const yaml =
        \\store: stores.sales
        \\format: csv
        \\output_dir: /data/exports
        \\entities:
        \\  - name: sales
        \\    role: parent
        \\    file: sales.csv
        \\    fields:
        \\      - { name: sale_id, type: int }
        \\      - { name: total, type: double }
        \\      - { name: date, type: datetime }
    ;

    var m = try parseManifestYaml(allocator, yaml);
    defer m.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 1), m.entities.len);
    try std.testing.expectEqual(@as(usize, 3), m.entities[0].fields.len);
    try std.testing.expectEqualStrings("sale_id", m.entities[0].fields[0].name);
    try std.testing.expectEqual(FieldType.int, m.entities[0].fields[0].field_type);
    try std.testing.expectEqualStrings("total", m.entities[0].fields[1].name);
    try std.testing.expectEqual(FieldType.double, m.entities[0].fields[1].field_type);
    try std.testing.expectEqualStrings("date", m.entities[0].fields[2].name);
    try std.testing.expectEqual(FieldType.datetime, m.entities[0].fields[2].field_type);
}

test "parseManifestYaml - missing store returns error" {
    const allocator = std.testing.allocator;
    const yaml =
        \\format: csv
        \\output_dir: /data/exports
    ;

    const result = parseManifestYaml(allocator, yaml);
    try std.testing.expectError(ManifestParseError.MissingStore, result);
}

test "parseManifestYaml - missing format returns error" {
    const allocator = std.testing.allocator;
    const yaml =
        \\store: stores.orders
    ;

    const result = parseManifestYaml(allocator, yaml);
    try std.testing.expectError(ManifestParseError.MissingFormat, result);
}

test "parseManifestYaml - findRoot and findChildren" {
    const allocator = std.testing.allocator;
    const yaml =
        \\store: stores.orders
        \\format: csv
        \\output_dir: /data
        \\entities:
        \\  - name: orders
        \\    role: parent
        \\    file: orders.csv
        \\    fields:
        \\      - { name: id, type: int }
        \\  - name: items
        \\    role: child
        \\    parent_field: items
        \\    join_key: id
        \\    file: items.csv
        \\    fields:
        \\      - { name: sku, type: string }
        \\  - name: attrs
        \\    role: child
        \\    parent: items
        \\    parent_field: attrs
        \\    join_key: sku
        \\    file: attrs.csv
        \\    fields:
        \\      - { name: key, type: string }
    ;

    var m = try parseManifestYaml(allocator, yaml);
    defer m.deinit(allocator);

    // findRoot
    const root = m.findRoot().?;
    try std.testing.expectEqualStrings("orders", root.name);

    // findChildren of root
    const children = try m.findChildren(allocator, "orders");
    defer allocator.free(children);
    try std.testing.expectEqual(@as(usize, 1), children.len);
    try std.testing.expectEqualStrings("items", children[0].name);

    // findChildren of items (grandchild)
    const grandchildren = try m.findChildren(allocator, "items");
    defer allocator.free(grandchildren);
    try std.testing.expectEqual(@as(usize, 1), grandchildren.len);
    try std.testing.expectEqualStrings("attrs", grandchildren[0].name);
}

test "parseManifestYaml - toImportSpec" {
    const allocator = std.testing.allocator;
    const yaml =
        \\store: stores.orders
        \\format: csv
        \\output_dir: /data
        \\entities:
        \\  - name: orders
        \\    role: parent
        \\    file: orders.csv
        \\    fields:
        \\      - { name: id, type: int }
        \\  - name: items
        \\    role: child
        \\    parent_field: items
        \\    join_key: id
        \\    file: items.csv
        \\    fields:
        \\      - { name: sku, type: string }
    ;

    var m = try parseManifestYaml(allocator, yaml);
    defer m.deinit(allocator);

    var spec = try m.toImportSpec(allocator);
    defer spec.deinit(allocator);

    try std.testing.expectEqualStrings("stores.orders", spec.target);
    try std.testing.expectEqual(ExportFormat.csv, spec.format);
    try std.testing.expect(spec.sources != null);
    const sources = spec.sources.?;
    try std.testing.expectEqual(@as(usize, 2), sources.len);
    try std.testing.expectEqualStrings("/data/orders.csv", sources[0].file);
    try std.testing.expectEqual(FileRole.parent, sources[0].role);
    try std.testing.expectEqualStrings("/data/items.csv", sources[1].file);
    try std.testing.expectEqual(FileRole.child, sources[1].role);
    try std.testing.expectEqualStrings("items", sources[1].embed_as.?);
    try std.testing.expectEqualStrings("id", sources[1].join_key.?);
}

test "toImportSpec - BSON format uses file_path not sources" {
    const allocator = std.testing.allocator;
    const yaml =
        \\store: stores.orders
        \\format: bson
        \\output_dir: /data/exports
        \\entities:
        \\  - name: orders
        \\    role: parent
        \\    file: orders.bson
        \\    fields:
        \\      - { name: id, type: int }
    ;

    var m = try parseManifestYaml(allocator, yaml);
    defer m.deinit(allocator);

    var spec = try m.toImportSpec(allocator);
    defer spec.deinit(allocator);

    try std.testing.expectEqualStrings("stores.orders", spec.target);
    try std.testing.expectEqual(ExportFormat.bson, spec.format);
    try std.testing.expectEqualStrings("/data/exports/orders.bson", spec.file_path.?);
    try std.testing.expectEqual(@as(?[]const ImportSourceEntry, null), spec.sources);
}

test "toImportSpec - JSON format without entities uses default file name" {
    const allocator = std.testing.allocator;
    const yaml =
        \\store: stores.orders
        \\format: json
        \\output_dir: /data/exports
    ;

    var m = try parseManifestYaml(allocator, yaml);
    defer m.deinit(allocator);

    var spec = try m.toImportSpec(allocator);
    defer spec.deinit(allocator);

    try std.testing.expectEqual(ExportFormat.json, spec.format);
    // No entities → format's default file name appended to output_dir.
    try std.testing.expectEqualStrings("/data/exports/export.json", spec.file_path.?);
}

test "toImportSpec - JSON with root entity" {
    const allocator = std.testing.allocator;
    const yaml =
        \\store: stores.orders
        \\format: json
        \\output_dir: /tmp/out
        \\entities:
        \\  - name: orders
        \\    role: parent
        \\    file: my_export.json
        \\    fields:
        \\      - { name: id, type: int }
    ;

    var m = try parseManifestYaml(allocator, yaml);
    defer m.deinit(allocator);

    var spec = try m.toImportSpec(allocator);
    defer spec.deinit(allocator);

    try std.testing.expectEqualStrings("/tmp/out/my_export.json", spec.file_path.?);
}

test "toImportSpec - BSON without output_dir uses bare file name" {
    const allocator = std.testing.allocator;
    const yaml =
        \\store: stores.orders
        \\format: bson
        \\entities:
        \\  - name: orders
        \\    role: parent
        \\    file: orders.bson
        \\    fields:
        \\      - { name: id, type: int }
    ;

    var m = try parseManifestYaml(allocator, yaml);
    defer m.deinit(allocator);

    var spec = try m.toImportSpec(allocator);
    defer spec.deinit(allocator);

    try std.testing.expectEqualStrings("orders.bson", spec.file_path.?);
}

test "parseManifestYaml - invalid format returns error" {
    const allocator = std.testing.allocator;
    const yaml =
        \\store: stores.orders
        \\format: xml
    ;

    const result = parseManifestYaml(allocator, yaml);
    try std.testing.expectError(ManifestParseError.InvalidFormat, result);
}

test "parseManifestYaml - comment lines are skipped" {
    const allocator = std.testing.allocator;
    const yaml =
        \\# This is a comment
        \\store: stores.orders
        \\# Another comment
        \\format: json
        \\output_dir: /data
    ;

    var m = try parseManifestYaml(allocator, yaml);
    defer m.deinit(allocator);

    try std.testing.expectEqualStrings("stores.orders", m.store);
    try std.testing.expectEqual(ExportFormat.json, m.format);
}
