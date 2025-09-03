#pragma once

namespace FastTri {
/**
 * @struct FaceInfo2
 * @brief Information associated with triangulation faces
 *
 * Used to track whether a face is inside or outside the constrained domain.
 */
class FaceInfo2 {
 public:
  constexpr FaceInfo2() noexcept = default;

  /**
   * @brief Checks if the face is inside the domain
   * @return True if inside domain, false otherwise
   */
  [[nodiscard]] constexpr auto is_in_domain() const noexcept -> bool {
    return m_in_domain;
  }

  /**
   * @brief Sets the domain status of the face
   * @param in_domain True to mark as inside domain, false otherwise
   */
  constexpr void set_in_domain(const bool in_domain) noexcept {
    m_in_domain = in_domain;
  }

 private:
  bool m_in_domain{false};  ///< Flag indicating if face is inside the domain
};
}  // namespace FastTri
