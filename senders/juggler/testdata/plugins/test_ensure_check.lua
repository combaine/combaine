function run()
    local result = {}
    for _, v in pairs(payload) do
        for cn, t in pairs(config.checks) do
            if t.metahost and t.metahost ~= v.Tags.metahost then break end
            if t.name and t.name ~= v.Tags.name then break end
            if t.type and t.type ~= v.Tags.type then break end

            for k, iv in pairs(v.Result) do
                if k:match(t.query) then
                    if t.percentile then iv = iv[t.percentile + 1] end
                    if iv > t.limit then
                        result[#result + 1] = {
                            tags = v.Tags,
                            description = string.format("%s.%s = %0.3f", v.Tags.metahost, k, iv),
                            level = t.status,
                            service = k,
                        }
                    end
                end
            end
        end
    end
    result[#result + 1] = {
        tags = {name="nonExisting", type="host"},
        description = "check not not present on juggler servers",
        level = "OK",
        service = "nonExisting",
    }
    return result
end
