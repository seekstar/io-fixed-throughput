from conan import ConanFile
from conan.tools.cmake import CMakeToolchain, CMake, cmake_layout, CMakeDeps


class io_fixed_throughputRecipe(ConanFile):
    name = "io-fixed-throughput"
    version = "0.1.0"

    # Optional metadata
    license = "GPLv3"
    author = "Jiansheng Qiu jianshengqiu.cs@gmail.com"
    url = "https://github.com/seekstar/io-fixed-throughput"

    # Binary configuration
    settings = "os", "compiler", "build_type", "arch"

    def requirements(self):
        self.requires("rusty-cpp/[>=0.1.9]")

    def layout(self):
        cmake_layout(self)

    def generate(self):
        deps = CMakeDeps(self)
        deps.generate()
        tc = CMakeToolchain(self)
        tc.generate()

    def build(self):
        cmake = CMake(self)
        cmake.configure()
        cmake.build()
