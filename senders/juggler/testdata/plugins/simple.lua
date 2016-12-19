local re = require("re")
local log = require("log")
local os = require("os")




sandbox = {
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
    env = {}
    for name, def in pairs(variables) do
        func = loadstring("return ".. def)
        setfenv(func, sandbox)
        ok, res = pcall(func)
        if ok then
            -- log.debug(string.format("for %s result is: %s: %s",name, ok,res))
            env[name] = res
        else
            log.error(string.format("Failed to eval %s: %s", def, res))
        end
    end
    return env
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
            -- print(string.format("Query %s on %s with key %s", q, iv, key))
            if key:match("^%-?%d$") then
                key = tonumber(key)
                if key < 0
                then key = #iv -- lua do not support keys -1 like python
                else key = key + 1 -- lua start index from 1, but python from 0
                end
            end
            iv = iv[key]
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

function run()
    testEnv = evalVariables()
    local result = {}
    for _, data in pairs(payload) do
        if not data.Tags or not data.Tags.aggregate then
           log.error("Missing tag 'aggregate'")
        else
            local pattern = "[$]{"..data.Tags.aggregate.."}"..[=[(?:[[][^]]+[]]|\.get\([^)]+\))+]=]
            for _, level in pairs {"CRIT", "WARN", "INFO", "OK"} do
                local check = conditions[level]
                if check then for _, case in pairs(check) do

                    local eval = case
                    local matched = false
                    for query in re.gmatch(case, pattern) do
                        matched = true
                        local metric = extractMetric(query, data.Result)
                        eval = replace(eval, query, tostring(metric))
                    end
                    if matched then
                        local test, err = loadstring("return "..eval)
                        setfenv(test, testEnv)

                        if err then
                            log.error(string.format("'%q' in check '%q' resoved to -> '%q'", err, case, eval))
                        else
                            ok, fire = pcall(test)
                            if not ok then
                                log.error("Error in query "..eval)
                            else
                                if fire then
                                    log.info("trigger test '"..eval.."' is true")
                                    result[#result + 1] = addResult(eval, level, data.Tags, env)
                                    break -- Checks are coupled with OR logic.
                                          -- break when one of expressions is evaluated as True
                                else
                                    log.info("trigger test '"..eval.."' is false")
                                end
                            end
                        end
                    else
                        -- log.debug("case '"..case.."' not match, pattern='"..pattern.."' for '"..level.."'")
                    end
                end end
            end
        end
    end
    return result
end
