-- Compatibility: Lua-5.1
function sumTable(t)
    local result = 0
    for _, v in pairs(t) do
        if type(v) == "table" then
            result = result + sumTable(v)
        else
            result = result + v
        end
    end
    return result
end

function flatting(key, val, res)
    if type(val) == "table" then
        if key ~= "" then
            key = key .. "/"
        end
        for k, v in pairs(val) do
            flatting(key .. k, v, res)
        end
    else
        res[key] = val
    end
end

function testQuery(t)
    local result = {}
    local flat = {}
    local path = split(query, "/")
    -- print(path[1], path[2], path[3])
    flatting("", t, flat)
    for k, v in pairs(flat) do
        local kp = split(k, "/")
        if #kp == #path then
            if k:match('^'..query..'$', 1) then
                result[#result + 1] = {
                    ["description"] = string.format("%s = %0.3f", table.concat(kp, ".", 2), v),
                    ["level"] = v,
                    ["service"] = kp[2],
                }
            end
        end
    end
    return result
end

function testEnv()
    -- we have payload
    if not _G.payload then
        return "Missing Payload"
    else
        -- and payload not empty
        payload_is_empty = true
        for _, v in pairs(payload) do
            if type(v) == "table" then
                payload_is_empty = false
                break
            end
        end
        if payload_is_empty then
            return "Payload empty"
        end
    end

    -- we have conditions
    if not _G.conditions then
        return "Missing Conditions"
    else
        -- OK case present in conditions
        if not conditions.OK then
            return "Missing OK case"
        end
        -- ok case not a table (array)
        if #conditions.OK <= 0 then
            return "OK case not array or empty"
        end
        -- ok cases is string
        for i, v in pairs(conditions.OK) do
            if type(v) ~= "string" then
                return string.format("OK case %d: %s is not a lua string", i, tostring(v))
            end
        end
    end

    -- Check Plugin Configs
    -- we have config
    if not _G.config then
        return "Missing plugin config"
    else
        -- we have config.checks
        if not config.checks then
            return "Missing checks"
        end
        -- we have testTimings in checks
        if not config.checks.testTimings then
            return "Missing testTimings from default test plugin config"
        end
        -- limit for testTimings is a number
        if type(config.checks.testTimings.limit) ~= "number" then
            return string.format("testTimings limit shoult be a number, not - %s: %s",
                                 type(config.checks.testTimings.limit),
                                 tostring(config.checks.testTimings.limit))
        end
    end


    return "OK"
end
