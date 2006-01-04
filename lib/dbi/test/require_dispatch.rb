$LOAD_PATH.unshift Dir.pwd + "/.."
alias old_require require
def require(file)
  if file =~ /^dbi\/(.*)/
    old_require $1
  else
    old_require file
  end
end
