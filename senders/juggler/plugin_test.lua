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
