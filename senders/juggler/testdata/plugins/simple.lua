local re = require("re")

function run()
    local result = {}
    for _, data in pairs(payload) do
        if not data.Tags or not data.Tags.aggregate then
            result[#result + 1] = {
                tags = data.Tags,
                level = "CRIT",
                error = "Missing tag 'aggregate'",
            }
        else
            local res = data.Result
            local pattern = [=[[$]{['\s]*]=]..data.Tags.aggregate..[=[[\s']*}]=]..
                [=[(?:[[][^]]+[]]|\.get\([^)]+\))+]=]
            for _, level in pairs {"CRIT", "WARN", "INFO", "OK"} do
                local check = conditions[level]
                if check then for _, case in pairs(check) do

                    local resolve = case
                    local matched = false
                    for query in re.gmatch(case, pattern) do
                        matched = true
                        local metric = "?"
                        if query:find("%.get%(") then
                            local key = query:match([[.get%(['"]([^'"]+)]])
                            metric = res[key] or 0
                        else
                            local iv = res
                            local key
                            for key in query:gmatch([==[%[['"%s]*([^"'%[%s%]]+)['"%s]*%]]==]) do
                                -- print("Query "..query.." key ".. tostring(key))
                                if key then
                                    iv = res[key]
                                end
                            end
                            metric = iv or 0
                        end
                        resolve = replace(resolve, query, tostring(metric))
                    end
                    if matched then
                        local test, err = loadstring("return "..resolve)
                        if err then
                            result[#result + 1] = {
                                tags = data.Tags,
                                description = checkDescription,
                                level = level,
                                service = checkName,
                                error = string.format("%q in check %q resoved to -> %q", err, case, resolve),
                            }
                        else
                            ok, fire = pcall(test)
                            if fire then
                                local q, v = resolve:match("(.+)([<>!]=?.*)")
                                local f, e = loadstring("return "..tostring(q))
                                local r
                                if not e then
                                    _, r = pcall(f)
                                    r = string.format("%0.3g%s", r, v)
                                else
                                    r = resolve
                                end
                                print(string.format("Eval Query is %s %s %s evaluated %s", resolve, q, v, r))

                                result[#result + 1] = {
                                    tags = data.Tags,
                                    description = r,
                                    level = level,
                                    service = checkName,
                                    error = "",
                                }
                                break -- this check is on fire, and case selected by or, skip next
                            end
                        end
                    end
                end end
            end
        end
    end
    return result
end

--[===[
function run()
    local result = {}
    for _, v in pairs(payload) do
        if not v.Tags or not v.Tags.aggregate then
            result[#result + 1] = {
                tags = v.Tags, level = "CRIT",
                error = "Missing tag 'aggregate'",
            }
        else
            local res = v.Result
            -- local pattern = [=[[$]{['"\s]*]=]..v.Tags.aggregate..[=[[\s'"]*}]=]..
            --     [=[(?:[[][^]]+[]]|\.get\([^)]+\))+]=]
            for _, level in pairs {"CRIT", "WARN", "INFO", "OK"} do
                local check = conditions[level]
                if check and #check > 0 then for _, case in pairs(check) do
                    print("Case "..case)
                    query = case:gsub([=[${['"%s]*]=]..v.Tags.aggregate..[=[[%s'"]*}]=], "res")
                    print("In [31m"..case.."[0m after gsub: [32m"..query.."[0m")
                end end
            end
        end
    end
    return result
end

result[#result + 1] = {
    tags = v.Tags,
    description = string.format("%s.%s = %0.3f", v.Tags.metahost, k, iv),
    level = t.status,
    service = k,
    error = "",
}
]===]--
