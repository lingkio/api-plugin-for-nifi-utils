
function makeQuery(template, data) {
  var query = '';
  var exists = false;
  var dataRow = {row:data};

  query = new Schnauzer(template).render(dataRow);

  return SqlString.format(query);
}

/**! @license schnauzer v1.3.0; Copyright (C) 2017-2018 by Peter Dematté */
// https://github.com/PitPik/Schnauzer/blob/master/schnauzer.js
(function defineSchnauzer(root, factory) {
  if (typeof exports === 'object') module.exports = factory(root);
  else if (typeof define === 'function' && define.amd) define('schnauzer', [],
    function () { return factory(root); });
  else root.Schnauzer = factory(root);
}(this, function SchnauzerFactory(root, undefined) { 'use strict';

var Schnauzer = function(template, options) {
    this.version = '1.3.0';
    this.options = {
      tags: ['{{', '}}'],
      entityMap: {
        '&': '&amp;',
        '<': '&lt;',
        '>': '&gt;',
        '"': '&quot;',
        "'": '&#39;',
        '/': '&#x2F;',
        '`': '&#x60;',
        '=': '&#x3D;'
      },
      doEscape: true,
      helpers: {},
      decorators: {},
      partials: {},
      recursion: 'self',
      characters: '$"<>%-=@',
      splitter: '|##|',
      tools: undefined, // hook for helpers/decorators
      render: undefined, // hook for shadow-DOM engines
    };
    init(this, options || {}, template);
  },
  init = function(_this, options, template) {
    for (var option in options) {
      _this.options[option] = options[option];
    }
    options = _this.options;

    _this.entityRegExp = new RegExp('[' + getKeys(options.entityMap, []).join('') + ']', 'g');
    switchTags(_this, options.tags);
    _this.helpers = options.helpers;
    _this.decorators = options.decorators;
    _this.partials = {};
    for (var name in options.partials) {
      _this.registerPartial(name, options.partials[name]);
    }
    if (template) {
      _this.registerPartial(options.recursion, template);
    }
  },
  isArray = Array.isArray || function(obj) { // obj instanceof Array;
    return obj && obj.constructor === Array;
  },
  isFunction = function(obj) {
    return obj && typeof obj === 'function';
  },
  getKeys = Object.keys || function(obj, keys) { // keys = []
    for (var key in obj) {
      obj.hasOwnProperty(key) && keys.push(key);
    }
    return keys;
  };

Schnauzer.prototype = {
  render: function(data, extra) {
    return this.partials[this.options.recursion](data, extra);
  },
  parse: function(text) {
    return this.registerPartial(this.options.recursion, text);
  },
  registerHelper: function(name, fn) {
    this.helpers[name] = fn;
  },
  unregisterHelper: function(name) {
    delete this.helpers[name];
  },
  registerDecorator: function(name, fn) {
    this.decorators[name] = fn;
  },
  unregisterDecorator: function(name, fn) {
    delete this.decorators[name];
  },
  registerPartial: function(name, text) {
    return this.partials[name] =
      (this.partials[name] || typeof text === 'function' ?
        text : sizzleTemplate(this, text, []));
  },
  unregisterPartial: function(name) {
    delete this.partials[name];
  },
  setTags: function(tags) {
    switchTags(this, tags);
  },
};

return Schnauzer;

function switchTags(_this, tags) {
  var _tags = tags[0] === '{{' ? ['{{2,3}', '}{2,3}'] : tags;
  var chars = _this.options.characters + '\\][';

  _this.inlineRegExp = new RegExp('(' + _tags[0] + ')' +
    '([>!&=])*\\s*([\\w\\'+ chars + '\\.]+)\\s*([\\w' + chars + '|\\.\\s]*)' + _tags[1], 'g');
  _this.sectionRegExp = new RegExp('(' + _tags[0] + ')([#^][*%]*)\\s*([\\w' + chars + ']*)' +
    '(?:\\s+([\\w$\\s|./' + chars + ']*))*(' + _tags[1] + ')((?:(?!\\1[#^])[\\S\\s])*?)' +
    '\\1\\/\\3\\5', 'g');
  _this.elseSplitter = new RegExp(_tags[0] + 'else' + _tags[1]);
}

function concat(array, newArray) { // way faster than [].concat
  for (var n = 0, l = array.length; n < l; n++) {
    newArray[newArray.length] = array[n];
  }
  return newArray;
}

function getSource(data, extra, newData, helpers) {
  var hasNewData = newData !== undefined;
  var isNew = !data.__schnauzerData;
  var _extra = hasNewData && !isNew && data.extra || [];
  var _helpers = !isNew && data.helpers || [];

  return {
    extra: extra ? concat(extra, _extra) : _extra,
    path: isNew ? [data] : hasNewData ? concat(data.path, [newData]) : data.path,
    helpers: helpers ? concat(_helpers, hasNewData && [helpers] || [{}]) : _helpers,
    __schnauzerData: true,
  };
}

function crawlObjectUp(data, keys) { // faster than while
  for (var n = 0, m = keys.length; n < m; n++) {
    if (keys[n] === './') continue; // TODO: check if deeper
    data = data && data[keys[n]];
  }
  return data;
}

function check(data, altData, keys) {
  return data !== undefined ? data : keys ?
    crawlObjectUp(altData, keys) : altData;
}

function findData(data, key, keys, pathDepth) {
  if (!keys) { // empty data;
    return;
  }

  var _data = data.path[pathDepth] || {};
  var helpers = data.helpers[pathDepth] || {};
  var value = check(helpers[key], helpers, keys);

  if (value === undefined) {
    value = crawlObjectUp(_data, keys);
  }
  if (value !== undefined) {
    return value;
  }
  for (var n = data.extra.length; n--; ) {
    value = check(data.extra[n][key], data.extra[n], keys);
    if (value !== undefined) {
      return value;
    }
  }
}

function getVar(text) {
  if (!text) { // speeds up parsing and live findData()
    return {};
  }

  var parts = text.split('=');
  var value = parts.length > 1 ? parts[1] : parts[0];
  var valueCharAt0 = value.charAt(0);
  var isString = valueCharAt0 === '"' || valueCharAt0 === "'";
  var isInline = false;
  var depth = 0;
  var keys = [];
  var path = [];
  var strict = false;
  var active = value.charAt(1) === '%' ? 2 : valueCharAt0 === '%' ? 1 : 0;
  var name = "";

  if (isString) {
    value = value.replace(/(?:^['"]|['"]$)/g, '');
  } else {
    value = active ? value.substr(active) : value;
    path = value.split('../');
    if (path.length > 1) {
      value = (path[0] === '@' && '@' || '') + path.pop();
      depth = path.length;
    }
    name = name.replace(/^(?:\.|this)\//, function() {
      strict = true;
      return '';
    });
    keys = value.split(/[\.\/]/);

    value = value.replace(/^(\.\/|this\.|this\/|\*)/, function(all, $1) {
      if ($1 === '*') {
        isInline = true;
        return '';
      }
      strict = true;
      keys[0] = './'; // findData() -> explicit
      return '';
    }).replace(/(?:^\[|\]$)/g, '');
  }

  return {
    name: parts.length > 1 ? parts[0] : value,
    value: value,
    isActive: active,
    isString: isString,
    isInline: isInline,
    strict: strict,
    keys: keys,
    depth: depth,
  };
}

function escapeHtml(string, _this) {
  return String(string).replace(_this.entityRegExp, function(char) {
    return _this.options.entityMap[char];
  });
}

function apply(_this, fn, name, params, data, parts, body, altBody) {
  return _this.options.tools ?
    _this.options.tools(_this, findData, getSource, fn, name, params, data, parts, body, altBody) :
    fn[isArray(params) || parts.isInline ? 'apply' : 'call']({
      getData: function getData(key) {
        key = parts.rawParts[key] || { value: key, keys: [key], depth: 0 };

        return key.isString ? key.value : findData(data, key.value, key.keys, key.depth);
      },
      escapeHtml: function escape(string) {
        return escapeHtml(string, _this);
      },
      getBody: function() {
        return body && body(data) || '';
      },
      gatAltBody: function() {
        return altBody && altBody(data) || '';
      },
      // data: data.path[0]
    }, parts.isInline ? [function() { return body || '' }, parts.parts, _this] : params);
}

function render(_this, part, data, fn, text, value, type) {
  var name = part.name;

  value = check(value, '');
  return _this.options.render ? apply(_this, _this.options.render, name, {
    name: name,
    data: data,
    section: !!part.section,
    partial: !!part.partial,
    isActive: part.isActive,
    fn: fn,
    text: text,
    value: value,
    parent: part.parent,
    type: (part.isInline && _this.decorators[name] && 'decorator') ||
      (part.partial && _this.partials[name] && 'partial') ||
      (_this.helpers[name] && 'helper') || type || '',
  }, data, part, fn) : text + value;
}

function splitVars(_this, vars, _data, unEscaped, char0) {
  var parts = {};
  var rawParts = {};
  var helpers = [];

  for (var n = 0, l = vars.length, tmp = {}; n < l; n++) {
    if (vars[n] === '') continue;
    if (vars[n] === 'as') {
      vars.splice(n, 1); // remove as from vars pool
      helpers = [vars.splice(n, 1)[n], vars.splice(n, 1)[n]];
      break;
    }
    tmp = getVar(vars[n]);
    parts[tmp.name] = tmp;
    rawParts[vars[n]] = tmp; // for apply.getData()
  }
  return {
    name: _data.name,
    vars: vars,
    parts: parts,
    rawParts: rawParts,
    partial: char0 === '>',
    isInline: _data.isInline,
    isUnescaped: !_this.options.doEscape || char0 === '&' || unEscaped,
    isActive: _data.isActive,
    depth: _data.depth,
    strict: _data.strict,
    keys: _data.keys,
    helpers: helpers,
  };
}

function createHelper(value, name, parent, helperData, len, n) {
  var helpers = len ? {
    '@index': n,
    '@last': n === len - 1,
    '@first': n === 0,
    '_parent': parent && [parent.name, n],
  } : {};

  helpers['@key'] = name;
  helpers['.'] = helpers['this'] = value;

  if (helperData.length !== 0) {
    helpers[helperData[0]] = value;
    helpers[helperData[1]] = name;
  }
  return helpers;
}

function inline(_this, text, sections, extType) {
  var parts = [];
  var splitter = _this.options.splitter;

  text = text.replace(_this.inlineRegExp, function(all, start, type, name, vars) {
    var char0 = type && type.charAt(0) || '';

    if (char0 === '!' || char0 === '=') {
      return '';
    }
    vars = vars.split(/\s+/); // split variables
    if (name === '-section-') {
      name = getVar(vars[1]);
      name.section = vars[0];
      parts.push(name);
      return splitter;
    }
    if (name === '*') {
      name = name + vars.shift();
    }
    parts.push(splitVars(_this, vars, getVar(name), start === '{{{', char0));
    return splitter;
  }).split(splitter);
  extType = getVar(extType).name; // remove %

  return function fastReplace(data, loopData) {
    return replace(_this, data, text, sections, extType, parts, loopData);
  };
}

function replace(_this, data, text, sections, extType, parts, loopData) {
  var out = '';
  var _out = '';
  var _fn = null;
  var _data = {};
  var part = {};
  var helper = {};
  var atom;

  for (var n = 0, l = text.length; n < l; n++) {
    part = parts[n];
    if (part === undefined) { // no other functions, just text
      out = extType ? render(_this, {}, data, _fn, out, text[n], extType) : out + text[n];
      continue;
    }
    out = out + text[n];
    if (part.section) { // from sizzleTemplate; -section-
      part.parent = loopData && crawlObjectUp(data.helpers, [0, '_parent']);
      out = render(_this, part, data, _fn = sections[part.section], out, _fn(data, loopData), extType);
      continue;
    }
    if (part.isInline) { // decorator
      out = apply(_this, _this.decorators[part.name || part.vars[0]],
        part.name, part.vars, data, part, out) || out;
      if (isFunction(out)) {
        out = out();
      }
      continue;
    }
    if (part.partial) { // partial -> executor
      helper = data.helpers[0] || (data.helpers[0] = {});
      atom = undefined;
      for (var key in part.parts) { // TODO: find better approach
        atom = part.parts[key];
        helper[key] = atom.keys.length && findData(data, atom.name, atom.keys, atom.depth) ||
          atom.isString && (atom.value || atom.name);
      }
      // atom &&  data.helpers.push(helper);
      _out = _this.partials[part.name](data);
      // atom && data.helpers.shift(); // TODO: check if still needed for dynamic vars...
    } else { // helpers and regular stuff
      part.parent = loopData && crawlObjectUp(data.helpers, [0, '_parent']);
      _fn = _replace(_this, part);
      _out = _fn(data);
    }
    out = render(_this, part, data, _fn, out, _out, extType);
  }

  return out;
}

function _replace(_this, part) {
  return function(data, keys) {
    var out = findData(data, part.name, keys || part.keys, part.depth);
    var fn = !part.strict && (_this.helpers[part.name] ||
      _this.partials[part.name]) || isFunction(out) && out;

    out = fn ? apply(_this, fn, part.name, part.vars, data, part) :
      out && (part.isUnescaped ? out : escapeHtml(out, _this));
    return out;
  }
}

function section(_this, fn, name, vars, unEscaped, isNot) {
  var type = name;

  name = getVar(vars.length && (name === 'if' || name === 'each' ||
    name === 'with' || name === 'unless') ? vars.shift() : name);
  vars = splitVars(_this, vars, getVar(name.name), unEscaped, '');

  return function fastLoop(data, loopData) {
    return loop(_this, data, fn, name, vars, isNot, type, loopData);
  };
}

function loop(_this, data, fn, name, vars, isNot, type, loopData) {
  var _data = findData(data, name.name, isArray(loopData) ? loopData : name.keys, name.depth);
  var helper = !name.strict && (_this.helpers[name.name] || isFunction(_data) && _data);
  var helperOut = helper && apply(_this, helper, name.name, vars.vars, data, vars, fn[0], fn[1]);
  var _isArray = isArray(_data);
  var objData = type === 'each' && !_isArray && typeof _data === 'object' && _data;
  var out = '';

  _data = _data === undefined ? isArray(data.path[0]) && data.path[0] : _data;

  if (helper) { // helpers or inline functions
    data.helpers[0] = createHelper(helperOut, name.name, undefined, vars.helpers);
    if (type === 'if') {
      return helperOut ? fn[0](data) : fn[1] && fn[1](data);
    } else if (type === 'unless') {
      return !helperOut ? fn[0](data) : fn[1] && fn[1](data);
    } else {
      _data = helperOut;
      _isArray = isArray(_data);
    }
  }
  if (type === 'unless') {
    _data = !_data;
  } else if (objData) {
    _data = getKeys(_data, []);
  }
  if (_isArray || objData) {
    if (isNot) {
      return !_data.length ? fn[0](_data) : '';
    }
    data.path.unshift({}); // faster then getSource()
    data.helpers.unshift({});
    for (var n = 0, l = _data.length; n < l; n++) {
      data.path[0] = _isArray ? _data[n] : objData[_data[n]];
      data.helpers[0] = createHelper(data.path[0], _isArray ? n : _data[n], name, vars.helpers, l, n);
      out = out + fn[0](data, _data[n]);
    }
    data.path.shift(); // jump back out of scope-level
    data.helpers.shift();
    return out;
  }

  if (isNot && !_data || !isNot && _data) { // regular replace

    return helper && typeof _data === 'string' ? _data : // comes from helper
      fn[0](type === 'unless' || type === 'if' ? data :
        getSource(data, undefined, _data, createHelper(_data, name.name, undefined, vars.helpers)));
  }

  return fn[1] && fn[1](data); // else
}

function sizzleTemplate(_this, text, sections) {
  var _text = '';
  var tags = _this.options.tags;

  while (_text !== text && (_text = text)) {
    text = text.replace(_this.sectionRegExp, function(all, start, type, name, vars, end, rest) {
      if (type === '#*') {
        var partialName = vars.replace(/(?:^['"]|['"]$)/g, '');
        _this.partials[partialName] = _this.partials[partialName] ||
          sizzleTemplate(_this, rest, sections);
        return '';
      }
      rest = rest.split(_this.elseSplitter); // .replace(/[\n\r]\s*$/, '')
      sections.push(section(_this,
        [inline(_this, rest[0], sections, name), rest[1] && inline(_this, rest[1], sections, name)],
        name, vars && vars.replace(/[(|)]/g, '').split(/\s+/) || [],
        start === '{{{', type === '^'));

      return (tags[0] + '-section- ' + (sections.length - 1) + ' ' + (vars || name) + tags[1]);
    });
  }
  text = inline(_this, text, sections);

  return function executor(data, extra) {
    return text(getSource(data, extra && (isArray(extra) && extra || [extra])));
  };
}
}));




// https://raw.githubusercontent.com/mysqljs/sqlstring/master/lib/SqlString.js
var SqlString = {};

var ID_GLOBAL_REGEXP    = /`/g;
var QUAL_GLOBAL_REGEXP  = /\./g;
var CHARS_GLOBAL_REGEXP = /[\0\b\t\n\r\x1a\"\'\\]/g; // eslint-disable-line no-control-regex
var CHARS_ESCAPE_MAP    = {
  '\0'   : '\\0',
  '\b'   : '\\b',
  '\t'   : '\\t',
  '\n'   : '\\n',
  '\r'   : '\\r',
  '\x1a' : '\\Z',
  '"'    : '\\"',
  '\''   : '\\\'',
  '\\'   : '\\\\'
};

SqlString.escapeId = function escapeId(val, forbidQualified) {
  if (Array.isArray(val)) {
    var sql = '';

    for (var i = 0; i < val.length; i++) {
      sql += (i === 0 ? '' : ', ') + SqlString.escapeId(val[i], forbidQualified);
    }

    return sql;
  } else if (forbidQualified) {
    return '`' + String(val).replace(ID_GLOBAL_REGEXP, '``') + '`';
  } else {
    return '`' + String(val).replace(ID_GLOBAL_REGEXP, '``').replace(QUAL_GLOBAL_REGEXP, '`.`') + '`';
  }
};

SqlString.escape = function escape(val, stringifyObjects, timeZone) {
  if (val === undefined || val === null) {
    return 'NULL';
  }

  switch (typeof val) {
    case 'boolean': return (val) ? 'true' : 'false';
    case 'number': return val + '';
    case 'object':
      if (val instanceof Date) {
        return SqlString.dateToString(val, timeZone || 'local');
      } else if (Array.isArray(val)) {
        return SqlString.arrayToList(val, timeZone);
      } else if (Buffer.isBuffer(val)) {
        return SqlString.bufferToString(val);
      } else if (typeof val.toSqlString === 'function') {
        return String(val.toSqlString());
      } else if (stringifyObjects) {
        return escapeString(val.toString());
      } else {
        return SqlString.objectToValues(val, timeZone);
      }
    default: return escapeString(val);
  }
};

SqlString.arrayToList = function arrayToList(array, timeZone) {
  var sql = '';

  for (var i = 0; i < array.length; i++) {
    var val = array[i];

    if (Array.isArray(val)) {
      sql += (i === 0 ? '' : ', ') + '(' + SqlString.arrayToList(val, timeZone) + ')';
    } else {
      sql += (i === 0 ? '' : ', ') + SqlString.escape(val, true, timeZone);
    }
  }

  return sql;
};

SqlString.format = function format(sql, values, stringifyObjects, timeZone) {
  if (values == null) {
    return sql;
  }

  if (!(values instanceof Array || Array.isArray(values))) {
    values = [values];
  }

  var chunkIndex        = 0;
  var placeholdersRegex = /\?+/g;
  var result            = '';
  var valuesIndex       = 0;
  var match;

  while (valuesIndex < values.length && (match = placeholdersRegex.exec(sql))) {
    var len = match[0].length;

    if (len > 2) {
      continue;
    }

    var value = len === 2
      ? SqlString.escapeId(values[valuesIndex])
      : SqlString.escape(values[valuesIndex], stringifyObjects, timeZone);

    result += sql.slice(chunkIndex, match.index) + value;
    chunkIndex = placeholdersRegex.lastIndex;
    valuesIndex++;
  }

  if (chunkIndex === 0) {
    // Nothing was replaced
    return sql;
  }

  if (chunkIndex < sql.length) {
    return result + sql.slice(chunkIndex);
  }

  return result;
};

SqlString.dateToString = function dateToString(date, timeZone) {
  var dt = new Date(date);

  if (isNaN(dt.getTime())) {
    return 'NULL';
  }

  var year;
  var month;
  var day;
  var hour;
  var minute;
  var second;
  var millisecond;

  if (timeZone === 'local') {
    year        = dt.getFullYear();
    month       = dt.getMonth() + 1;
    day         = dt.getDate();
    hour        = dt.getHours();
    minute      = dt.getMinutes();
    second      = dt.getSeconds();
    millisecond = dt.getMilliseconds();
  } else {
    var tz = convertTimezone(timeZone);

    if (tz !== false && tz !== 0) {
      dt.setTime(dt.getTime() + (tz * 60000));
    }

    year       = dt.getUTCFullYear();
    month       = dt.getUTCMonth() + 1;
    day         = dt.getUTCDate();
    hour        = dt.getUTCHours();
    minute      = dt.getUTCMinutes();
    second      = dt.getUTCSeconds();
    millisecond = dt.getUTCMilliseconds();
  }

  // YYYY-MM-DD HH:mm:ss.mmm
  var str = zeroPad(year, 4) + '-' + zeroPad(month, 2) + '-' + zeroPad(day, 2) + ' ' +
    zeroPad(hour, 2) + ':' + zeroPad(minute, 2) + ':' + zeroPad(second, 2) + '.' +
    zeroPad(millisecond, 3);

  return escapeString(str);
};

SqlString.bufferToString = function bufferToString(buffer) {
  return 'X' + escapeString(buffer.toString('hex'));
};

SqlString.objectToValues = function objectToValues(object, timeZone) {
  var sql = '';

  for (var key in object) {
    var val = object[key];

    if (typeof val === 'function') {
      continue;
    }

    sql += (sql.length === 0 ? '' : ', ') + SqlString.escapeId(key) + ' = ' + SqlString.escape(val, true, timeZone);
  }

  return sql;
};

SqlString.raw = function raw(sql) {
  if (typeof sql !== 'string') {
    throw new TypeError('argument sql must be a string');
  }

  return {
    toSqlString: function toSqlString() { return sql; }
  };
};

function escapeString(val) {
  var chunkIndex = CHARS_GLOBAL_REGEXP.lastIndex = 0;
  var escapedVal = '';
  var match;

  while ((match = CHARS_GLOBAL_REGEXP.exec(val))) {
    escapedVal += val.slice(chunkIndex, match.index) + CHARS_ESCAPE_MAP[match[0]];
    chunkIndex = CHARS_GLOBAL_REGEXP.lastIndex;
  }

  if (chunkIndex === 0) {
    // Nothing was escaped
    return "'" + val + "'";
  }

  if (chunkIndex < val.length) {
    return "'" + escapedVal + val.slice(chunkIndex) + "'";
  }

  return "'" + escapedVal + "'";
}

function zeroPad(number, length) {
  number = number.toString();
  while (number.length < length) {
    number = '0' + number;
  }

  return number;
}

function convertTimezone(tz) {
  if (tz === 'Z') {
    return 0;
  }

  var m = tz.match(/([\+\-\s])(\d\d):?(\d\d)?/);
  if (m) {
    return (m[1] === '-' ? -1 : 1) * (parseInt(m[2], 10) + ((m[3] ? parseInt(m[3], 10) : 0) / 60)) * 60;
  }
  return false;
}

/**********
ACTUAL FLOW
**********/

var flowFile = session.get();
if (flowFile != null) {
  var columns = ""
  var rows = ""

  var StreamCallback =  Java.type("org.apache.nifi.processor.io.StreamCallback")
  var IOUtils = Java.type("org.apache.commons.io.IOUtils")
  var StandardCharsets = Java.type("java.nio.charset.StandardCharsets")

  flowFile = session.write(flowFile,
    new StreamCallback(function(inputStream, outputStream) {

      var text = IOUtils.toString(inputStream, StandardCharsets.UTF_8)
      var body = JSON.parse(text)

      
      var query = ''
      if (body.data.length > 0) {
        //  data, replace strings as much as possible
        for (var i=0; i<body.data.length; i++) {
          query += makeQuery(body.query, body.data[i]);
        }
      } else {
        query = body.query;
      }

      outputStream.write(query.getBytes(StandardCharsets.UTF_8))
    })) // end streamcallback

   session.transfer(flowFile, REL_SUCCESS)
 
}
