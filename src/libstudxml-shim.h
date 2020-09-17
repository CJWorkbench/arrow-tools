#ifndef LIBSTUDXML_SHIM_H
#define LIBSTUDXML_SHIM_H

// xlnt relies on libstudxml for XML parsing. And xlnt does not wrap
// exceptions that it requests libstudxml to generate.
//
// That's a failing of libxlnt. But the only thing we need from libstudxml
// is its parser-exception declaration. So we can declare it here instead,
// and we won't need to change xlnt at all.

namespace xml
{
    class exception : public std::exception {};
}

#endif /* LIBSTUDXML_SHIM_H */
