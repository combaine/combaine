local re = require("re")

function run()
    local result = {}
    for _, v in pairs(payload) do
        if not v.Tags or not v.Tags.aggregate then
            result[#result + 1] = {
                tags = v.Tags, level = "CRIT",
                error = "Missing tag 'aggregate'",
            }
        else for _, level in pairs {"CRIT", "WARN", "INFO", "OK"} do

            check = conditions[level]
            if check then for _, case in pairs(check) do

                pattern = [=[[$]{\s*]=]..v.Tags.aggregate..[[\s*}]]
                pattern = pattern..[=[(?:[[][^]]+[]]|\.get\([^)]+\))+]=]

                for query in re.gmatch(case, pattern) do
                    print("In [31m"..case.."[0m Matched query: [32m"..query.."[0m")
                end
            end end
        end end
    end
    return result
end

--[[
result[#result + 1] = {
    tags = v.Tags,
    description = string.format("%s.%s = %0.3f", v.Tags.metahost, k, iv),
    level = t.status,
    service = k,
    error = "",
}
]]--
