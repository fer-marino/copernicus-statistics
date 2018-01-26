package com.serco.sentinel1.wamp.config

/*
@EnableWebFluxSecurity
class SecurityConfig {

    //@Bean @Profile("development")
    fun userDetailsService(): MapReactiveUserDetailsService {
        val user = User.withDefaultPasswordEncoder()
                .username("user")
                .password("user")
                .roles("USER")
                .build()
        return MapReactiveUserDetailsService(user)
    }

    //@Bean
    fun springSecurityFilterChain(http: ServerHttpSecurity): SecurityWebFilterChain {
        http
                .authorizeExchange()
                    .anyExchange().authenticated()
                    .and()
                .httpBasic().and()
                .formLogin()
        return http.build()
    }

    @Autowired @Profile("production")
    @Throws(Exception::class)
    fun configureGlobal(auth: AuthenticationManagerBuilder) {
        auth
                .ldapAuthentication()
                .userDnPatterns("uid={0},ou=people")
                .groupSearchBase("ou=groups")
    }

}
*/