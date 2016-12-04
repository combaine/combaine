-- Compatibility: Lua-5.1
function split(str, pat)
   local t = {}  -- NOTE: use {n = 0} in Lua-5.0
   local fpat = "(.-)" .. pat
   local last_end = 1
   local s, e, cap = str:find(fpat, 1)
   while s do
      if s ~= 1 or cap ~= "" then
         table.insert(t,cap)
      end
      last_end = e+1
      s, e, cap = str:find(fpat, last_end)
   end
   if last_end <= #str then
      cap = str:sub(last_end)
      table.insert(t, cap)
   end
   return t
end

function split_path(str)
   return split(str,'[/]+')
end

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
    local path = split_path(query)
    -- print(path[1], path[2], path[3])
    flatting("", t, flat)
    for k, v in pairs(flat) do
        local kp = split_path(k)
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
    result[#result + 1] = {}
    return result

end

