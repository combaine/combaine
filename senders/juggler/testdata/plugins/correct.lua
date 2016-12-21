function run(t)
    local result = {}
    result[#result + 1] = {
        tags = {
            name = "TestHost",
            type = "host",
            metahost = "TestMetahost",
        },
        service = "Test Service",
        level = "OK",
        description = "Test description",
    }
    return result
end
