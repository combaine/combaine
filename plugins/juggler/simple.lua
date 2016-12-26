local re = require("re")
local log = require("log")
local os = require("os")

-- global vars
--
-- payload        - table come from juggler sender
--
-- checkName      - string come from juggler config
-- checkDescription   - string come from juggler config
-- variables      - table come from juggler config
-- config         - table come from juggler config (user defined config)
-- conditions       - table come from juggler config OK WARN CRIT ...

local envVars = {}      -- evaled vars from variables
local sandbox = {       -- sandbox for function evalVariables
  iftimeofday = function (bot, top, in_val, out_val)
    function inTimeOfDay()
      hour = tonumber(os.date("%H"))
      if bot < top then
        return bot <= hour and hour <= top
      else
        return bot <= hour or hour < top
      end
    end

    if inTimeOfDay(bot, top) then
      return in_val
    else
      return out_val
    end
  end,
}

function evalVariables()
  for name, def in pairs(variables) do
    func = loadstring("return ".. def)
    setfenv(func, sandbox)
    ok, res = pcall(func)
    if ok then
      -- log.debug(string.format("for %s result is: %s: %s",name, ok,res))
      envVars[name] = res
    else
      log.error(string.format("Failed to eval %s: %s", def, res))
    end
  end
end


function extractMetric(q, res)
  local metric = "?"
  if q:find("%.get%(") then
    local key = q:match([[.get%(['"]([^'"]+)]])
    metric = res[key] or 0
  else
    local iv = res
    local key
    for key in q:gmatch([==[%[['"%s]*([^"'%[%s%]]+)['"%s]*%]]==]) do
      -- log.debug(string.format("Query %s on %s with key %s", q, iv, key))
      if key:match("^%-?%d$") then
        key = tonumber(key)
        if key < 0
        then key = #iv -- lua do not support keys -1 like python
        else key = key + 1 -- lua start index from 1, but python from 0
        end
      end
      iv = iv[key]
      if not iv then break end -- payload not contains sub table for given key
    end
    metric = iv or 0
  end
  return metric
end

function addResult(ev, lvl, tags)
  local q, v = ev:match("(.+)([<>!]=?.*)")
  local f, e = loadstring("return "..tostring(q))
  if type(f) == "function" then
    setfenv(f, {})
  end
  local r
  if not e then
    _, r = pcall(f)
    r = string.format("%0.3f%s", r, v)
  else
    r = ev
  end
  r = r:gsub("%s+", "")
  log.debug(string.format("Eval Query is '%s' q='%s' v='%s' evaluated r='%s'", ev, q, v, r))

  return {
    tags = tags,
    description = r,
    level = lvl,
    service = checkName,
  }
end

function evalCheck(cases, lvl, d)
  local pattern = "[$]{"..d.Tags.aggregate.."}"..[=[(?:[[][^]]+[]]|\.get\([^)]+\))+]=]
  local STRMpattern = "%${"..d.Tags.aggregate.."}.iteritems%(%)%s+if%s+%(k"
  local anchor = "%s+if[^%.]+%.startswith%('([^']+)'%)%s+and[^%.]+%.endswith%('([^']+)'%)"
  for _, c in pairs(cases) do
    local eval = c
    local matched = false
    if c:match(STRMpattern) then
      log.debug("STRM pattern detected")
      -- STRM pattern
      -- "sum([v for k,v in ${agg}.iteritems() if (k.startswith() and k.endswith())])
      --    / sum([v for k,v in ${agg}.iteritems() if (k.startswith() and k.endswith())]) >  0"
      for query in c:gmatch("(sum%b())") do
        local nStart, nEnd = query:match(anchor)
        if nStart and nEnd then
          matched = true
          local subPattern = nStart..".*"..nEnd
          local metric = 0
          for k, v in pairs(d.Result) do
            if type(v) == "number" and k:match(subPattern) then
              metric = metric + v
            end
          end
          eval = replace(eval, query, tostring(metric))
        end
      end
    else
      for query in re.gmatch(c, pattern) do
        matched = true
        local metric = extractMetric(query, d.Result)
        eval = replace(eval, query, tostring(metric))
      end
    end
    if matched then
      local test, err = loadstring("return "..eval)
      if type(test) == "function" then
        setfenv(test, envVars) -- envVars defined at top
      end

      if err then
        log.error(string.format("'%q' in check '%q' resoved to -> '%q'", err, c, eval))
      else
        local ok, fire = pcall(test)
        if not ok then
          log.error("Error in query "..eval)
        else
          log.info(string.format("%s trigger test `%s` -> `%s` is %s", lvl, c, eval, fire))
          if fire then
            -- Checks are coupled with OR logic.
            -- return when one of expressions is evaluated as True
            return addResult(eval, lvl, d.Tags)
          end
        end
      end
    else
      log.debug(string.format("case '%s' not match, pattern='%s' for '%s'",c, pattern, lvl))
    end
  end
end

function run()
  evalVariables()   -- now eval variables
  local result = {}
  for _, data in pairs(payload) do
    if not data.Tags or not data.Tags.aggregate then
       log.error("Missing tag 'aggregate'")
    else
      for _, level in pairs {"CRIT", "WARN", "INFO", "OK"} do
        local check = conditions[level]
        if check then
          local res = evalCheck(check, level, data)
          if res then
            result[#result +1] = res
            break -- higher level event set on fire, no need continue checks
          end
        end
      end
    end
  end
  return result
end
